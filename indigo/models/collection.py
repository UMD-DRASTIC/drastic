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
import json
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
import paho.mqtt.publish as publish
import logging

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
from indigo.acl import serialize_acl_metadata
from indigo.models.errors import (
    CollectionConflictError,
    ResourceConflictError,
    NoSuchCollectionError
)


class Collection(Model):
    """Collection model"""
    id = columns.Text(default=default_cdmi_id, index=True)
    container = columns.Text(primary_key=True, required=False)
    name = columns.Text(primary_key=True, required=True)
    metadata = columns.Map(columns.Text, columns.Text, index=True)
    create_ts = columns.DateTime()
    modified_ts = columns.DateTime()
    is_root = columns.Boolean(default=False, index=True)

    # The access columns contain lists of group IDs that are allowed
    # the specified permission. If the lists have at least one entry
    # then access is restricted, if there are no entries in a particular
    # list, then access is granted to all (authenticated users)
    read_access = columns.List(columns.Text)
    edit_access = columns.List(columns.Text)
    write_access = columns.List(columns.Text)
    delete_access = columns.List(columns.Text)

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
        res.mqtt_publish('create')

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
        return root

    def mqtt_publish(self, operation):
        payload = dict()
        payload['id'] = self.id
        payload['container'] = self.container
        payload['name'] = self.name
        payload['create_ts'] = self.create_ts
        payload['modified_ts'] = self.modified_ts
        payload['metadata'] = meta_cassandra_to_cdmi(self.metadata)
        topic = u'{2}/collection{0}/{1}'.format(self.container, self.name, operation)
        # Clean up the topic by removing superfluous slashes.
        topic = '/'.join(filter(None, topic.split('/')))
        # Remove MQTT wildcards from the topic. Corner-case: If the collection name is made entirely of # and + and a
        # script is set to run on such a collection name. But that's what you get if you use stupid names for things.
        topic = topic.replace('#', '').replace('+', '')
        logging.info(u'Publishing on topic "{0}"'.format(topic))
        publish.single(topic, json.dumps(payload, default=datetime_serializer))

    def delete(self):
        #self.mqtt_publish('delete')
        super(Collection, self).delete()

    @classmethod
    def delete_all(cls, path):
        """Delete recursively all sub-collections and all resources contained
        in a collection at 'path'"""
        parent_coll = Collection.find_by_path(path)
        if not parent_coll:
            return
        colls = list(parent_coll.get_child_collections())
        rescs = list(parent_coll.get_child_resources())
        for resc in rescs:
            resc.delete()
        for coll in colls:
            Collection.delete_all(coll.path())
        parent_coll.delete()

    @classmethod
    def find(cls, path):
        """Return a collection from a path"""
        return cls.find_by_path(path)

    @classmethod
    def find_by_id(cls, id_string):
        """Return a collection from a uuid"""
        return cls.objects.filter(id=id_string).first()

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
        return cls.objects.filter(is_root=True).first()

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
        kwargs['modified_ts'] = datetime.now()

        if 'metadata' in kwargs:
            kwargs['metadata'] = meta_cdmi_to_cassandra(kwargs['metadata'])

        self.mqtt_publish('update')

        return super(Collection, self).update(**kwargs)

    def user_can(self, user, action):
        """
        User can perform the action if any of the user's group IDs
        appear in this list for 'action'_access in this object.
        """
        if user.administrator:
            return True
        l = getattr(self, '{}_access'.format(action))
        if len(l) and not len(user.groups):
            # Group access required, user not in any groups
            return False
        if not len(l):
            # Group access not required
            return True

        # if groups has less than user.groups then it has had a group
        # removed, it confirms presence in l
        groups = set(user.groups) - set(l)
        return len(groups) < len(user.groups)
