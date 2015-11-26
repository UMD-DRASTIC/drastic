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

import json
import logging

import paho.mqtt.publish as publish
from cassandra.cqlengine import (
    columns,
    connection
)
from cassandra.cqlengine.models import Model
from datetime import datetime

from indigo import get_config
from indigo.models.acl import (
    Ace,
    acemask_to_str,
    cdmi_str_to_aceflag,
    str_to_acemask,
    cdmi_str_to_acemask,
    serialize_acl_metadata
)
from indigo.models.errors import (
    CollectionConflictError,
    ResourceConflictError,
    NoSuchCollectionError
)
from indigo.models.group import Group
from indigo.models.id_index import IDIndex
from indigo.models.resource import Resource
from indigo.util import (
    decode_meta,
    default_cdmi_id,
    meta_cassandra_to_cdmi,
    meta_cdmi_to_cassandra,
    merge,
    metadata_to_list,
    split,
    datetime_serializer
)


class Collection(Model):
    """Collection model"""
    id = columns.Text(default=default_cdmi_id)
    container = columns.Text(primary_key=True, required=False)
    name = columns.Text(primary_key=True, required=True)
    metadata = columns.Map(columns.Text, columns.Text, index=True)
    create_ts = columns.DateTime()
    modified_ts = columns.DateTime()
    is_root = columns.Boolean(default=False)
    acl = columns.Map(columns.Text, columns.UserDefinedType(Ace))

    # The access columns contain lists of group IDs that are allowed
    # the specified permission. If the lists have at least one entry
    # then access is restricted, if there are no entries in a particular
    # list, then access is granted to all (authenticated users)
#     read_access = columns.List(columns.Text)
#     edit_access = columns.List(columns.Text)
#     write_access = columns.List(columns.Text)
#     delete_access = columns.List(columns.Text)

    @classmethod
    def create(cls, **kwargs):
        """Create a new collection

        We intercept the create call"""
        # TODO: Allow name starting or ending with a space ?
#         container = kwargs.get('container', '/').strip()
#         name = kwargs.get('name').strip()
#
#         kwargs['name'] = name
#         kwargs['container'] = container

        name = kwargs.get('name')
        container = kwargs.get('container', '/')
        kwargs['container'] = container

        d = datetime.now()
        kwargs['create_ts'] = d
        kwargs['modified_ts'] = d

        if 'metadata' in kwargs:
            kwargs['metadata'] = meta_cdmi_to_cassandra(kwargs['metadata'])

        # Check if parent collection exists
        parent = Collection.find_by_path(container)

        if parent is None:
            raise NoSuchCollectionError(container)

        resource = Resource.find_by_path(merge(container, name))

        if resource is not None:
            raise ResourceConflictError(container)
        collection = Collection.find_by_path(merge(container, name))

        if collection is not None:
            raise CollectionConflictError(container)

        res = super(Collection, cls).create(**kwargs)
        try:
            state = res.mqtt_get_state()
            res.mqtt_publish('create', {}, state)
        except Exception as e:
            pass
        # Create a row in the ID index table
        idx = IDIndex.create(id=res.id,
                             classname="indigo.models.collection.Collection",
                             key=res.path())
        return res

    @classmethod
    def create_root(cls):
        """Create the root container"""
        d = datetime.now()
        root = Collection(container='null',
                          name='Home',
                          is_root=True,
                          create_ts=d,
                          modified_ts=d)
        root.save()
        root.add_default_acl()
        return root

    def add_default_acl(self):
        # Add read access to all authenticated users
        self.update_acl(["AUTHENTICATED@"], [])

    def delete_acl(self):
        self.acl = None
        self.save()

    def mqtt_get_state(self):
        payload = dict()
        payload['id'] = self.id
        payload['container'] = self.container
        payload['name'] = self.name
        payload['create_ts'] = self.create_ts
        payload['modified_ts'] = self.modified_ts
        payload['metadata'] = meta_cassandra_to_cdmi(self.metadata)

        return payload

    def mqtt_publish(self, operation, pre_state, post_state):
        payload = dict()
        payload['pre'] = pre_state
        payload['post'] = post_state
        topic = u'{0}/collection{1}/{2}'.format(operation, self.container, self.name)
        # Clean up the topic by removing superfluous slashes.
        topic = '/'.join(filter(None, topic.split('/')))
        # Remove MQTT wildcards from the topic. Corner-case: If the collection name is made entirely of # and + and a
        # script is set to run on such a collection name. But that's what you get if you use stupid names for things.
        topic = topic.replace('#', '').replace('+', '')
        logging.info(u'Publishing on topic "{0}"'.format(topic))
        publish.single(topic, json.dumps(payload, default=datetime_serializer))

    def delete(self):
        state = self.mqtt_get_state()
        self.mqtt_publish('delete', state, {})
        idx = IDIndex.find(self.id)
        if idx:
            idx.delete()
        super(Collection, self).delete()

    @classmethod
    def delete_all(cls, path):
        """Delete recursively all sub-collections and all resources contained
        in a collection at 'path'"""
        parent = Collection.find_by_path(path)

        if not parent:
            return

        collections = list(parent.get_child_collections())
        resources = list(parent.get_child_resources())

        for resource in resources:
            resource.delete()

        for collection in collections:
            Collection.delete_all(collection.path())

        parent.delete()

    @classmethod
    def find(cls, path):
        """Return a collection from a path"""
        return cls.find_by_path(path)

    @classmethod
    def find_by_id(cls, id_string):
        """Return a collection from a uuid"""
        idx = IDIndex.find(id_string)
        if idx:
            if idx.classname == "indigo.models.collection.Collection":
                return cls.find(idx.key)
        return None

    @classmethod
    def find_by_path(cls, path):
        """Return a collection from a path"""
        if path == '/':
            return cls.get_root_collection()
        container, name = split(path)
        return cls.objects.filter(container=container, name=name).first()

    @classmethod
    def find_by_name(cls, name):
        """Return a collection from a name"""
        return cls.objects.filter(name=name).first()

    @classmethod
    def get_root_collection(cls):
        """Return the root collection"""
        return cls.objects.filter(container='null',name='Home').first()

    def __unicode__(self):
        return self.path()

    def get_child_collections(self):
        """Return a list of all sub-collections"""
        return Collection.objects.filter(container=self.path()).all()

    def get_child_collection_count(self):
        """Return the number of sub-collections"""
        return Collection.objects.filter(container=self.path()).count()

    def get_child_resources(self):
        """Return a list of all resources"""
        return Resource.objects.filter(container=self.path()).all()

    def get_child_resource_count(self):
        """Return the number of resources"""
        return Resource.objects.filter(container=self.path()).count()

    def get_metadata(self):
        """Return a dictionary of metadata"""
        return meta_cassandra_to_cdmi(self.metadata)

    def get_metadata_key(self, key):
        """Return the value of a metadata"""
        return decode_meta(self.metadata.get(key, ""))

    def get_parent_collection(self):
        """Return the parent collection"""
        return Collection.find_by_path(self.container)

    def get_acl_metadata(self):
        """Return a dictionary of acl based on the Collection schema"""
        return serialize_acl_metadata(self)

    def md_to_list(self):
        """Transform metadata to a list of couples for web ui"""
        return metadata_to_list(self.metadata)

    def path(self):
        """Return the full path of the collection"""
        if self.is_root:
            return u"/"
        else:
            return merge(self.container, self.name)

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
            "id": self.id,
            "container": self.container,
            "name": self.name,
            "path": self.path(),
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
        pre_state = self.mqtt_get_state()
        kwargs['modified_ts'] = datetime.now()

        if 'metadata' in kwargs:
            kwargs['metadata'] = meta_cdmi_to_cassandra(kwargs['metadata'])

        super(Collection, self).update(**kwargs)
        try:
            post_state = self.mqtt_get_state()

            if pre_state['metadata'] == post_state['metadata']:
                self.mqtt_publish('update_object', pre_state, post_state)
            else:
                self.mqtt_publish('update_metadata', pre_state, post_state)
        except Exception as e :
            pass

        return self
    def create_acl(self, read_access, write_access):
        self.acl = {}

        self.save()
        self.update_acl(read_access, write_access)

    def update_acl(self, read_access, write_access):
        """Replace the acl with the given list of access.

        read_access: a list of groups id that have read access for this
                     collection
        write_access: a list of groups id that have write access for this
                     collection

        """
        # The dictionary keys are the groups id for which we have an ACE
        # We don't use aceflags yet, everything will be inherited by lower
        # sub-collections
        # acemask is set with helper (read/write - see indigo/models/acl/py)
        cfg = get_config(None)
        keyspace = cfg.get('KEYSPACE', 'indigo')
        access = {}
        for gid in read_access:
            access[gid] = "read"
        for gid in write_access:
            if gid in access:
                access[gid] = "read/write"
            else:
                access[gid] = "write"
        ls_access = []
        for gid in access:
            g = Group.find_by_id(gid)
            if g:
                ident = g.name
            elif gid.upper() == "AUTHENTICATED@":
                ident = "AUTHENTICATED@"
            else:
                # TODO log or return error if the identifier isn't found ?
                continue
            s = ("'{}': {{"
                 "acetype: 'ALLOW', "
                 "identifier: '{}', "
                 "aceflags: {}, "
                 "acemask: {}"
                 "}}").format(gid, ident, 0, str_to_acemask(access[gid], False))
            ls_access.append(s)
        acl = "{{{}}}".format(", ".join(ls_access))
        query= ("UPDATE {}.collection SET acl = acl + {}"
                "WHERE container='{}' AND name='{}'").format(
            keyspace,
            acl,
            self.container,
            self.name)
        connection.execute(query)

    def update_cdmi_acl(self, cdmi_acl):
        """Update acl with the metadata acl passed with a CDMI request"""
        cfg = get_config(None)
        keyspace = cfg.get('KEYSPACE', 'indigo')
        ls_access = []
        for cdmi_ace in cdmi_acl:
            g = Group.find(cdmi_ace['identifier'])
            if g:
                ident = g.name
            elif gid.upper() == "AUTHENTICATED@":
                ident = "AUTHENTICATED@"
            else:
                # TODO log or return error if the identifier isn't found ?
                continue
            s = ("'{}': {{"
                 "acetype: '{}', "
                 "identifier: '{}', "
                 "aceflags: {}, "
                 "acemask: {}"
                 "}}").format(g.id,
                              cdmi_ace['acetype'].upper(),
                              ident,
                              cdmi_str_to_aceflag(cdmi_ace['aceflags']),
                              cdmi_str_to_acemask(cdmi_ace['acemask'], False)
                             )
            ls_access.append(s)
        acl = "{{{}}}".format(", ".join(ls_access))
        query= ("UPDATE {}.collection SET acl = acl + {}"
                "WHERE container='{}' AND name='{}'").format(
            keyspace,
            acl,
            self.container,
            self.name)
        connection.execute(query)

    def get_authorized_actions(self, user):
        """"Get available actions for user according to a group"""
        # Check permission on the parent container if there's no action
        # defined at this level
        if not self.acl:
            if self.is_root:
                return set([])
            else:
                parent_container = Collection.find(self.container)
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
