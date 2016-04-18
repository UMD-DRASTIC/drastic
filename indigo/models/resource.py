"""Resource Model

Copyright 2015 Archive Analytics Solutions

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from datetime import datetime
import logging

from indigo.models import (
    DataObject,
    TreeEntry
)
from indigo.models.acl import (
    acemask_to_str,
    serialize_acl_metadata
)
from indigo.models.errors import (
    NoSuchCollectionError,
    ResourceConflictError
)
from indigo.util import (
    decode_meta,
    default_cdmi_id,
    merge,
    meta_cdmi_to_cassandra,
    meta_cassandra_to_cdmi,
    metadata_to_list,
    split,
)


class Resource(object):
    """Resource Model"""

    logger = logging.getLogger('database')


    def __init__(self, entry, obj=None):
        self.entry = entry
        self.obj = obj
        # Tree metadata
        self.tree_metadata = meta_cassandra_to_cdmi(self.entry.metadata)
        # Object metadata, only populated when needed as it requires an extra
        # Cassandra request
        self.metadata = None
        self.acl = self.entry.container_acl
        self._id = self.entry.id
        self.name = self.entry.name
        self.parent = self.entry.container
        self.path = self.entry.path()
        self.url = self.entry.url
        self.mimetype = self.tree_metadata.get("cdmi_mimetype", "")
        self.is_reference = not self.url.startswith("cassandra://")
        self.is_internal = self.url.startswith("cassandra://")


    def __unicode__(self):
        return self.path


    def chunk_content(self):
        """Get a chunk of the data object"""
        self.get_obj()
        return self.obj.chunk_content()


    @classmethod
    def create(cls, container, name, uuid=None, metadata=None,
               url=None, mimetype=None):
        """Create a new resource in the tree_entry table"""
        from indigo.models import Collection
        if uuid is None:
            uuid = default_cdmi_id()
        create_ts = datetime.now()
        modified_ts = create_ts
        path = merge(container, name)
        if metadata:
            metadata = meta_cdmi_to_cassandra(metadata)
        # Check the container exists
        collection = Collection.find(container)
        if not collection:
            raise NoSuchCollectionError(container)
        # Make sure parent/name are not in use.
        existing = cls.find(path)
        if existing:
            raise ResourceConflictError(path)
        data_entry = TreeEntry.create(container=container,
                                      name=name,
                                      container_create_ts=create_ts,
                                      container_modified_ts=modified_ts,
                                      url=url,
                                      id=uuid,
                                      mimetype=mimetype)
        data_entry.save()
        return Resource(data_entry)


    def create_acl(self, read_access, write_access):
        """Add the ACL from lists of group ids, ACL are replaced"""
        self.get_obj()
        self.obj.create_acl(read_access, write_access)


    def delete(self):
        """Delete the resource in the tree_entry table and all the corresponding
        blobs"""
        self.delete_blobs()
        self.entry.delete()


    def delete_blobs(self):
        """Delete all blobs of the corresponding uuid"""
        if self.is_internal:
            obj_id = self.get_obj_id()
            if obj_id:
                DataObject.delete_id(obj_id)


    @classmethod
    def find(cls, path):
        """Return a resource from a path"""
        coll_name, resc_name = split(path)
        entries = TreeEntry.objects.filter(container=coll_name, name=resc_name)
        if not entries:
            return None
        else:
            return cls(entries.first())


    def full_dict(self, user=None):
        """Return a dictionary which describes a resource for the web ui"""
        self.get_obj()
        data = {
            "id": self._id,
            "name": self.get_name(),
            "container": self.parent,
            "path": self.path,
            "metadata": self.md_to_list(),
            "url": self.url,
            "is_reference": self.is_reference,
            "mimetype": self.mimetype or "application/octet-stream",
            "type": self.mimetype,
        }
        # Add fields when the object isn't a reference
        if self.obj:
            data["checksum"] = self.obj.checksum
            data["size"] = self.obj.size
            data["create_ts"] = self.obj.create_ts
            data["modified_ts"] = self.obj.modified_ts
        if user:
            data['can_read'] = self.user_can(user, "read")
            data['can_write'] = self.user_can(user, "write")
            data['can_edit'] = self.user_can(user, "edit")
            data['can_delete'] = self.user_can(user, "delete")
        return data


    def get_acl_metadata(self):
        """Return a dictionary of acl based on the Resource schema"""
        return serialize_acl_metadata(self)


    def get_authorized_actions(self, user):
        """"Get available actions for user according to a group"""
        # Check permission on the parent container if there's no action
        # defined at this level
        if not self.acl:
            from indigo.models import Collection
            parent_container = Collection.find(self.parent)
            return parent_container.get_authorized_actions(user)
        actions = set([])
        for gid in user.groups:
            if gid in self.acl:
                ace = self.acl[gid]
                level = acemask_to_str(ace.acemask, True)
                if level == "read":
                    actions.add("read")
                elif level == "write":
                    actions.add("write")
                    actions.add("delete")
                    actions.add("edit")
                elif level == "read/write":
                    actions.add("read")
                    actions.add("write")
                    actions.add("delete")
                    actions.add("edit")
        return actions


    def get_metadata(self):
        """Return the metadata associated to the object as a CDMI dictionary
        """
        self.get_obj()
        return meta_cassandra_to_cdmi(self.entry.metadata)


    def get_metadata_key(self, key):
        """Return the value of a metadata"""
        self.get_obj()
        return decode_meta(self.entry.metadata.get(key, ""))


    def get_name(self):
        """Return the name of a resource. If the resource is a reference we
        append a trailing '?' on the resource name"""
        # References are object whose url doesn't start with 'cassandra://'
        if self.is_reference:
            return "{}?".format(self.name)
        else:
            return self.name


    def get_obj(self):
        """Get the row in the data_object table. If it's a reference it my be
        null"""
        if not self.obj:
            if self.is_internal:
                obj_id = self.get_obj_id()
                self.obj = DataObject.find(obj_id)
                if self.obj:
                    self.metadata = self.obj.metadata


    def get_obj_id(self):
        """Get the data object id from the url"""
        if self.is_internal:
            return self.url.replace("cassandra://", "")
        else:
            return None

    def get_path(self):
        """Return the full path of the resource"""
        return self.path


    def md_to_list(self):
        """Transform metadata to a list of couples for web ui"""
        return metadata_to_list(self.entry.metadata)


    def read_acl(self):
        """Return two list of groups id which have read and write access"""
        read_access = []
        write_access = []
        for gid, ace in self.acl.items():
            oper = acemask_to_str(ace.acemask, True)
            if oper == "read":
                read_access.append(gid)
            elif oper == "write":
                write_access.append(gid)
            elif oper == "read/write":
                read_access.append(gid)
                write_access.append(gid)
            else:
                # Unknown combination
                pass
        return read_access, write_access


    def simple_dict(self, user=None):
        """Return a dictionary which describes a resource for the web ui"""
        data = {
            "id": self._id,
            "name": self.get_name(),
            "container": self.parent,
            "path": self.path,
            "is_reference": self.is_reference,
            "mimetype": self.mimetype or "application/octet-stream",
            "type": self.mimetype,
        }
        if user:
            data['can_read'] = self.user_can(user, "read")
            data['can_write'] = self.user_can(user, "write")
            data['can_edit'] = self.user_can(user, "edit")
            data['can_delete'] = self.user_can(user, "delete")
        return data


    def update(self, **kwargs):
        """Update a resource"""
        if 'metadata' in kwargs:
            kwargs['metadata'] = meta_cdmi_to_cassandra(kwargs['metadata'])
        self.entry.update(**kwargs)
        if self.is_internal:
            self.get_obj()
            kwargs2 = {'modified_ts' : datetime.now()}
            if self.obj:
                self.obj.update(**kwargs2)
        return self


    def update_cdmi_acl(self, cdmi_acl):
        """Update the ACL from a cdmi list of ACE"""
        if self.is_internal:
            self.get_obj()
            if self.obj:
                self.obj.update_cdmi_acl(cdmi_acl)


    def user_can(self, user, action):
        """
        User can perform the action if any of the user's group IDs
        appear in this list for 'action'_access in this object.
        """
        if user.administrator:
            # An administrator can do anything
            return True
        actions = self.get_authorized_actions(user)
        if action in actions:
            return True
        return False


