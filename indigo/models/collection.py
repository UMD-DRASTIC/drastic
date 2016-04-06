"""Collection Model

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
from cassandra.cqlengine import connection
from cassandra.query import SimpleStatement

from indigo import get_config
from indigo.models import TreeEntry
from indigo.models.acl import (
    acemask_to_str,
)
from indigo.util import (
    meta_cassandra_to_cdmi,
    meta_cdmi_to_cassandra,
    metadata_to_list,
    merge,
    split,
)
from indigo.models.errors import (
    CollectionConflictError,
    ResourceConflictError,
    NoSuchCollectionError
)

# import json
# from cassandra.cqlengine import (
#     columns,
#     connection
# )
# from cassandra.cqlengine.models import Model
# import paho.mqtt.publish as publish
# import logging
# 
# from indigo.models.group import Group
# from indigo.models.resource import Resource
# from indigo.models.search import SearchIndex
# from indigo.models.acl import (
#     Ace,
#     acemask_to_str,
#     cdmi_str_to_aceflag,
#     str_to_acemask,
#     cdmi_str_to_acemask,
#     serialize_acl_metadata
# )
# from indigo.util import (
#     decode_meta,
#     default_cdmi_id,
#     meta_cassandra_to_cdmi,
#     meta_cdmi_to_cassandra,
#     merge,
#     metadata_to_list,
#     split,
#     datetime_serializer
# )



class Collection(object):
    """Collection model"""

    def __init__(self, entry):
        self.entry = entry

        self.is_root = (self.entry.name == "." and self.entry.container == '/')
        # Get name
        if self.is_root:
            self.name = u"Home"
        else:
            _, self.name = split(self.entry.container)
        self.metadata = self.entry.container_metadata
        self.path = self.entry.container
        self.parent, _ = split(self.path)
        self._id = self.entry.container_id
        self.create_ts = self.entry.container_create_ts
        self.acl = self.entry.container_acl


    @classmethod
    def create(cls, name, container='/', metadata=None):
        """Create a new collection"""
        from indigo.models import Resource
        path = merge(container, name)
        # Check if parent collection exists
        parent = Collection.find(container)
        if parent is None:
            raise NoSuchCollectionError(container)

        #TODO: Check resource doesn't exist
        resource = Resource.find(merge(container, name))
        if resource is not None:
            raise ResourceConflictError(container)

        collection = Collection.find(path)
        if collection is not None:
            raise CollectionConflictError(container)
        
        
        if metadata:
            metadata = meta_cdmi_to_cassandra(metadata)
            
        d = datetime.now()
        coll_entry = TreeEntry.create(container=path,
                                      name='.',
                                      container_create_ts=d,
                                      container_modified_ts=d)
        coll_entry.save()
        coll_entry.id = coll_entry.container_id
        coll_entry.save()
        
        child_entry = TreeEntry.create(container=container,
                                       name=name + '/',
                                       id=coll_entry.container_id)
        child_entry.save()
        return Collection.find(path)


    def create_acl(self, read_access, write_access):
        self.entry.create_acl(read_access, write_access)

    @classmethod
    def create_root(cls):
        """Create the root container"""
        d = datetime.now()
        root_entry = TreeEntry.create(container='/',
                                      name='.',
                                      container_create_ts=d,
                                      container_modified_ts=d)
        root_entry.save()
        root_entry.id = root_entry.container_id
        root_entry.save()
        
        root_entry.add_default_acl()
        return root_entry


    def delete(self):
        cfg = get_config(None)
        session = connection.get_session()
        keyspace = cfg.get('KEYSPACE', 'indigo')
        session.set_keyspace(keyspace)
        query = SimpleStatement("""DELETE FROM tree_entry WHERE container=%s""")
        session.execute(query, (self.path,))
        # Get the row that describe the collection as a child of its parent
        child = TreeEntry.objects.filter(container=self.parent,
                                           name="{}/".format(self.name)).first()
        if child:
            child.delete()


    @classmethod
    def delete_all(cls, path):
        """Delete recursively all sub-collections and all resources contained
        in a collection at 'path'"""
        from indigo.models import Resource
        parent = Collection.find(path)

        if not parent:
            return

        collections, resources = parent.get_child()
        collections = [Collection.find(merge(path,c)) for c in collections]
        resources = [Resource.find(merge(path,c)) for c in resources]

        for resource in resources:
            resource.delete()
# 
        for collection in collections:
            Collection.delete_all(collection.path)

        parent.delete()


    @classmethod
    def find(cls, path):
        entries = TreeEntry.objects.filter(container=path, name=".")
        if not entries:
            return None
        else:
            return cls(entries.first())


    def get_authorized_actions(self, user):
        """"Get available actions for user according to a group"""
        # Check permission on the parent container if there's no action
        # defined at this level
        if not self.acl:
            if self.is_root:
                return set([])
            else:
                parent_container = Collection.find(self.parent)
                return parent_container.get_authorized_actions(user)
        actions = set([])
        for gid in user.groups + ["AUTHENTICATED@"]:
            if gid in self.acl:
                ace = self.acl[gid]
                level = acemask_to_str(ace.acemask, False)
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


    def get_child(self):
        """Return two lists for child container and child dataobjects"""
        entries = TreeEntry.objects.filter(container=self.path)
        child_container = []
        child_dataobject = []
        for entry in list(entries):
            if entry.name == '.':
                continue
            elif entry.name.endswith('/'):
                child_container.append(entry.name[:-1])
            else:
                child_dataobject.append(entry.name)
        return (child_container , child_dataobject)


    def get_metadata(self):
        """Return a dictionary of metadata"""
        return meta_cassandra_to_cdmi(self.metadata)


    def md_to_list(self):
        """Transform metadata to a list of couples for web ui"""
        return metadata_to_list(self.metadata)
 
    def read_acl(self):
        """Return two list of groups id which have read and write access"""
        read_access = []
        write_access = []
        for gid, ace in self.acl.items():
            op = acemask_to_str(ace.acemask, False)
            if op == "read":
                read_access.append(gid)
            elif op == "write":
                write_access.append(gid)
            elif op == "read/write":
                read_access.append(gid)
                write_access.append(gid)
            else:
                # Unknown combination
                pass
        return read_access, write_access


    def to_dict(self, user=None):
        """Return a dictionary which describes a collection for the web ui"""
        data = {
            "id": self._id,
            "container": self.path,
            "name": self.name,
            "path": self.path,
            "created": self.create_ts,
            "metadata": self.md_to_list()
        }
        if user:
            data['can_read'] = self.user_can(user, "read")
            data['can_write'] = self.user_can(user, "write")
            data['can_edit'] = self.user_can(user, "edit")
            data['can_delete'] = self.user_can(user, "delete")
        return data


    def update(self, **kwargs):
        """Update a collection"""
        kwargs['container_modified_ts'] = datetime.now()
        if 'metadata' in kwargs:
            kwargs['container_metadata'] = meta_cdmi_to_cassandra(kwargs['metadata'])
            del kwargs['metadata']
        self.entry.update(**kwargs)
        return self


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

#     def index(self):
#         SearchIndex.reset(self.id)
#         SearchIndex.index(self, ['name', 'metadata'])
# 
# 
#     def get_child_collections(self):
#         """Return a list of all sub-collections"""
#         return Collection.objects.filter(container=self.path()).all()
# 
#     def get_child_collection_count(self):
#         """Return the number of sub-collections"""
#         return Collection.objects.filter(container=self.path()).count()
# 
#     def get_child_resources(self):
#         """Return a list of all resources"""
#         return Resource.objects.filter(container=self.path()).all()
# 
#     def get_child_resource_count(self):
#         """Return the number of resources"""
#         return Resource.objects.filter(container=self.path()).count()
# 
#     def get_metadata(self):
#         """Return a dictionary of metadata"""
#         return meta_cassandra_to_cdmi(self.metadata)
# 
#     def get_metadata_key(self, key):
#         """Return the value of a metadata"""
#         return decode_meta(self.metadata.get(key, ""))
# 
# 
#     def get_acl_metadata(self):
#         """Return a dictionary of acl based on the Collection schema"""
#         return serialize_acl_metadata(self)
# 
# 
# 
#     def update_acl(self, read_access, write_access):
#         """Replace the acl with the given list of access.
# 
#         read_access: a list of groups id that have read access for this
#                      collection
#         write_access: a list of groups id that have write access for this
#                      collection
# 
#         """
#         # The dictionary keys are the groups id for which we have an ACE
#         # We don't use aceflags yet, everything will be inherited by lower
#         # sub-collections
#         # acemask is set with helper (read/write - see indigo/models/acl/py)
#         cfg = get_config(None)
#         keyspace = cfg.get('KEYSPACE', 'indigo')
#         access = {}
#         for gid in read_access:
#             access[gid] = "read"
#         for gid in write_access:
#             if gid in access:
#                 access[gid] = "read/write"
#             else:
#                 access[gid] = "write"
#         ls_access = []
#         for gid in access:
#             g = Group.find_by_id(gid)
#             if g:
#                 ident = g.name
#             elif gid.upper() == "AUTHENTICATED@":
#                 ident = "AUTHENTICATED@"
#             else:
#                 # TODO log or return error if the identifier isn't found ?
#                 continue
#             s = ("'{}': {{"
#                  "acetype: 'ALLOW', "
#                  "identifier: '{}', "
#                  "aceflags: {}, "
#                  "acemask: {}"
#                  "}}").format(gid, ident, 0, str_to_acemask(access[gid], False))
#             ls_access.append(s)
#         acl = "{{{}}}".format(", ".join(ls_access))
#         query= ("UPDATE {}.collection SET acl = acl + {}"
#                 "WHERE container='{}' AND name='{}'").format(
#             keyspace,
#             acl,
#             self.container.replace("'", "\''"),
#             self.name.replace("'", "\''"))
#         connection.execute(query)
# 
#     def update_cdmi_acl(self, cdmi_acl):
#         """Update acl with the metadata acl passed with a CDMI request"""
#         cfg = get_config(None)
#         keyspace = cfg.get('KEYSPACE', 'indigo')
#         ls_access = []
#         for cdmi_ace in cdmi_acl:
#             if 'identifier' in cdmi_ace:
#                 gid = cdmi_ace['identifier']
#             else:
#                 # Wrong syntax for the ace
#                 continue
#             g = Group.find(gid)
#             if g:
#                 ident = g.name
#             elif gid.upper() == "AUTHENTICATED@":
#                 ident = "AUTHENTICATED@"
#             else:
#                 # TODO log or return error if the identifier isn't found ?
#                 continue
#             s = ("'{}': {{"
#                  "acetype: '{}', "
#                  "identifier: '{}', "
#                  "aceflags: {}, "
#                  "acemask: {}"
#                  "}}").format(g.id,
#                               cdmi_ace['acetype'].upper(),
#                               ident,
#                               cdmi_str_to_aceflag(cdmi_ace['aceflags']),
#                               cdmi_str_to_acemask(cdmi_ace['acemask'], False)
#                              )
#             ls_access.append(s)
#         acl = "{{{}}}".format(", ".join(ls_access))
#         query= ("UPDATE {}.collection SET acl = acl + {}"
#                 "WHERE container='{}' AND name='{}'").format(
#             keyspace,
#             acl,
#             self.container.replace("'", "\'"),
#             self.name.replace("'", "\'"))
#         connection.execute(query)

