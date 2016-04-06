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
from cassandra.cqlengine import connection
from cassandra.query import SimpleStatement

from indigo import get_config
from indigo.models import (
    DataObject,
    TreeEntry
)
from indigo.models.acl import (
    acemask_to_str,
)
from indigo.util import (
    merge,
    meta_cdmi_to_cassandra,
    meta_cassandra_to_cdmi,
    split,
)
# import json
# from cassandra.cqlengine import (
#     columns,
#     connection
# )
# from cassandra.cqlengine.models import Model
# from paho.mqtt import publish
# 
# from indigo import get_config
# from indigo.models.errors import (
#     NoSuchCollectionError,
#     ResourceConflictError
# )
# from indigo.models.group import Group
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
# from indigo.models.search import SearchIndex
# 
# import indigo.drivers


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
        self.container = self.entry.container
        self.path = self.entry.path()
        self.url = self.entry.url
        self.mimetype = self.tree_metadata.get("cdmi_mimetype", "")
        self.is_reference = not self.url.startswith("cassandra://")



    @classmethod
    def create(cls, container, name, id, metadata=None, url=None, mimetype=None):
        from indigo.models import Collection
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

        d = datetime.now()
        data_entry = TreeEntry.create(container=container,
                                      name=name,
                                      container_create_ts=d,
                                      container_modified_ts=d,
                                      url=url,
                                      id=id,
                                      mimetype=mimetype)
        data_entry.save()
        return data_entry


    def delete(self):
        cfg = get_config(None)
        session = connection.get_session()
        keyspace = cfg.get('KEYSPACE', 'indigo')
        session.set_keyspace(keyspace)
        query = SimpleStatement("""DELETE FROM data_object WHERE id=%s""")
        session.execute(query, (self._id,))
        self.entry.delete()


    @classmethod
    def find(cls, path):
        """Return a resource from a path"""
        coll_name, resc_name = split(path)
        entries = TreeEntry.objects.filter(container=coll_name, name=resc_name)
        if not entries:
            return None
        else:
            return cls(entries.first())


    def get_authorized_actions(self, user):
        """"Get available actions for user according to a group"""
        # Check permission on the parent container if there's no action
        # defined at this level
        if not self.acl:
            from indigo.models import Collection
            parent_container = Collection.find(self.container)
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
        self.get_obj()
        return meta_cassandra_to_cdmi(self.entry.metadata)


    def get_obj(self):
        if not self.obj:
            self.obj = DataObject.find(self._id)


    def simple_dict(self, user=None):
        """Return a dictionary which describes a resource for the web ui"""
        data = {
            "id": self._id,
            "name": self.name,
            "container": self.container,
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

    def full_dict(self, user=None):
        """Return a dictionary which describes a resource for the web ui"""
        self.get_obj()
        data = {
            "id": self._id,
            "name": self.name,
            "container": self.container,
            "path": self.path,
            "checksum": self.obj.checksum,
            "size": self.obj.size,
            "metadata": self.get_metadata(),
            "create_ts": self.obj.create_ts,
            "modified_ts": self.obj.modified_ts,
            "url": self.url,
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


    def read_acl(self):
        """Return two list of groups id which have read and write access"""
        read_access = []
        write_access = []
        for gid, ace in self.acl.items():
            op = acemask_to_str(ace.acemask, True)
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


    def update(self, **kwargs):
        """Update a resource"""
        if 'metadata' in kwargs:
            kwargs['container_metadata'] = meta_cdmi_to_cassandra(kwargs['metadata'])
            del kwargs['metadata']
        self.entry.update(**kwargs)
        
        self.get_obj()
        kwargs2 = {'modified_ts' : datetime.now()}
        self.obj.update(**kwargs2)
        
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

#     def mqtt_get_state(self):
#         payload = dict()
#         payload['id'] = self.id
#         payload['url'] = self.url
#         payload['container'] = self.container
#         payload['name'] = self.get_name()
#         payload['create_ts'] = self.create_ts
#         payload['modified_ts'] = self.modified_ts
#         payload['metadata'] = meta_cassandra_to_cdmi(self.metadata)
# 
#         return payload
# 
#     def mqtt_publish(self, operation, pre_state, post_state):
#         payload = dict()
#         payload['pre'] = pre_state
#         payload['post'] = post_state
#         topic = u'{0}/resource{1}/{2}'.format(operation, self.container, self.get_name())
#         # Clean up the topic by removing superfluous slashes.
#         topic = '/'.join(filter(None, topic.split('/')))
#         # Remove MQTT wildcards from the topic. Corner-case: If the resource name is made entirely of # and + and a
#         # script is set to run on such a resource name. But that's what you get if you use stupid names for things.
#         topic = topic.replace('#', '').replace('+', '')
#         logging.info(u'Publishing on topic "{0}"'.format(topic))
#         try:
#             publish.single(topic, json.dumps(payload, default=datetime_serializer))
#         except:
#             logging.error(u'Problem while publishing on topic "{0}"'.format(topic))
# 



#     def __unicode__(self):
#         return self.path()
# 
#     def get_acl_metadata(self):
#         """Return a dictionary of acl based on the Resource schema"""
#         return serialize_acl_metadata(self)
# 
#     def get_container(self):
#         """Returns the parent collection of the resource"""
#         # Check the container exists
#         from indigo.models.collection import Collection
#         container = Collection.find_by_path(self.container)
#         if not container:
#             raise NoSuchCollectionError(self.container)
#         else:
#             return container
# 
#     def get_metadata(self):
#         """Return a dictionary of metadata"""
#         return meta_cassandra_to_cdmi(self.metadata)
# 
#     def get_metadata_key(self, key):
#         """Return the value of a metadata"""
#         return decode_meta(self.metadata.get(key, ""))
# 
#     def get_name(self):
#         """Return the name of a resource. If the resource is a reference we
#         append a trailing '?' on the resource name"""
#         # References are object whose url doesn't start with 'cassandra://'
#         if self.is_reference():
#             return "{}?".format(self.name)
#         else:
#             return self.name
# 
#     def index(self):
#         SearchIndex.reset(self.id)
#         SearchIndex.index(self, ['name', 'metadata', 'mimetype'])
# 
#     def is_reference(self):
#         return not self.url.startswith("cassandra://")
# 
#     def md_to_list(self):
#         """Transform metadata to a list of couples for web ui"""
#         return metadata_to_list(self.metadata)
# 
#     def path(self):
#         """Return the full path of the resource"""
#         return merge(self.container, self.name)
# 

    def chunk_content(self):
        self.get_obj()
        return self.obj.chunk_content()
        
# 

# 
 
    def create_acl(self, read_access, write_access):
        self.get_obj()
        self.obj.create_acl(read_access, write_access)
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
#                               cdmi_str_to_acemask(cdmi_ace['acemask'], True)
#                              )
#             ls_access.append(s)
#         acl = "{{{}}}".format(", ".join(ls_access))
#         query= ("UPDATE {}.resource SET acl = acl + {}"
#                 "WHERE container='{}' AND name='{}'").format(
#             keyspace,
#             acl,
#             self.container.replace("'", "\''"),
#             self.name.replace("'", "\''"))
#         connection.execute(query)
