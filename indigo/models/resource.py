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
import json
import logging
from cassandra.cqlengine import (
    columns,
    connection
)
from cassandra.cqlengine.models import Model
from paho.mqtt import publish

from indigo import get_config
from indigo.models.errors import (
    NoSuchCollectionError,
    ResourceConflictError
)
from indigo.models.group import Group
from indigo.models.acl import (
    Ace,
    acemask_to_str,
    cdmi_str_to_aceflag,
    str_to_acemask,
    cdmi_str_to_acemask,
    serialize_acl_metadata
)
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
from indigo.models.id_index import IDIndex
from indigo.models.search import SearchIndex

import indigo.drivers


class Resource(Model):
    """Resource Model"""
    id = columns.Text(default=default_cdmi_id, required=True)
    container = columns.Text(primary_key=True, required=True)
    name = columns.Text(primary_key=True, required=True)
    checksum = columns.Text(required=False)
    size = columns.BigInt(required=False, default=0)
    metadata = columns.Map(columns.Text, columns.Text, index=True)
    mimetype = columns.Text(required=False)
    url = columns.Text(required=False)
    alt_url = columns.Set(columns.Text, required=False)
    create_ts = columns.DateTime()
    modified_ts = columns.DateTime()
    type = columns.Text(required=False, default='UNKNOWN')
    acl = columns.Map(columns.Text, columns.UserDefinedType(Ace))

    # The access columns contain lists of group IDs that are allowed
    # the specified permission. If the lists have at least one entry
    # then access is restricted, if there are no entries in a particular
    # list, then access is granted to all (authenticated users)
#     read_access = columns.List(columns.Text)
#     edit_access = columns.List(columns.Text)
#     write_access = columns.List(columns.Text)
#     delete_access = columns.List(columns.Text)

    logger = logging.getLogger('database')

    @classmethod
    def create(cls, **kwargs):
        """Create a new resource

        When we create a resource, the minimum we require is a name
        and a container. There is little chance of getting trustworthy
        versions of any of the other data at creation stage.
        """

        # TODO: Allow name starting or ending with a space ?
#         kwargs['name'] = kwargs['name'].strip()
        kwargs['create_ts'] = datetime.now()
        kwargs['modified_ts'] = kwargs['create_ts']

        if 'metadata' in kwargs:
            kwargs['metadata'] = meta_cdmi_to_cassandra(kwargs['metadata'])

        # Check the container exists
        from indigo.models.collection import Collection
        collection = Collection.find_by_path(kwargs['container'])

        if not collection:
            raise NoSuchCollectionError(kwargs['container'])

        # Make sure parent/name are not in use.
        existing = cls.objects.filter(container=kwargs['container'],
                                      name=kwargs['name']).first()
        if existing:
            raise ResourceConflictError(merge(kwargs['container'],
                                              kwargs['name']))

        resource = super(Resource, cls).create(**kwargs)
        state = resource.mqtt_get_state()
        resource.mqtt_publish('create', {}, state)
        resource.index()
        # Create a row in the ID index table
        idx = IDIndex.create(id=resource.id,
                             classname="indigo.models.resource.Resource",
                             key=resource.path())

        return resource

    def mqtt_get_state(self):
        payload = dict()
        payload['id'] = self.id
        payload['url'] = self.url
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
        topic = u'{0}/resource{1}/{2}'.format(operation, self.container, self.name)
        # Clean up the topic by removing superfluous slashes.
        topic = '/'.join(filter(None, topic.split('/')))
        # Remove MQTT wildcards from the topic. Corner-case: If the resource name is made entirely of # and + and a
        # script is set to run on such a resource name. But that's what you get if you use stupid names for things.
        topic = topic.replace('#', '').replace('+', '')
        logging.info(u'Publishing on topic "{0}"'.format(topic))
        publish.single(topic, json.dumps(payload, default=datetime_serializer))

    def delete(self):
        driver = indigo.drivers.get_driver(self.url)
        driver.delete_blob()
        state = self.mqtt_get_state()
        self.mqtt_publish('delete', state, {})
        idx = IDIndex.find(self.id)
        if idx:
            idx.delete()
        SearchIndex.reset(self.id)
        super(Resource, self).delete()

    @classmethod
    def find(cls, path):
        """Return a resource from a path"""
        return cls.find_by_path(path)

    @classmethod
    def find_by_id(cls, id_string):
        """Return a resource from a uuid"""
        idx = IDIndex.find(id_string)
        if idx:
#            if idx.classname == "indigo.models.resource.Resource":
            if idx.classname.endswith("Resource"):
                return cls.find(idx.key)
        return None

    @classmethod
    def find_by_path(cls, path):
        """Find resource by path"""
        coll_name, resc_name = split(path)
        return cls.objects.filter(container=coll_name, name=resc_name).first()

    def __unicode__(self):
        return self.path()

    def get_acl_metadata(self):
        """Return a dictionary of acl based on the Resource schema"""
        return serialize_acl_metadata(self)

    def get_container(self):
        """Returns the parent collection of the resource"""
        # Check the container exists
        from indigo.models.collection import Collection
        container = Collection.find_by_path(self.container)
        if not container:
            raise NoSuchCollectionError(self.container)
        else:
            return container

    def get_metadata(self):
        """Return a dictionary of metadata"""
        return meta_cassandra_to_cdmi(self.metadata)

    def get_metadata_key(self, key):
        """Return the value of a metadata"""
        return decode_meta(self.metadata.get(key, ""))

    def index(self):
        SearchIndex.reset(self.id)
        SearchIndex.index(self, ['name', 'metadata', 'mimetype'])

    def md_to_list(self):
        """Transform metadata to a list of couples for web ui"""
        return metadata_to_list(self.metadata)

    def path(self):
        """Return the full path of the resource"""
        return merge(self.container, self.name)

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

    def to_dict(self, user=None):
        """Return a dictionary which describes a resource for the web ui"""
        data = {
            "id": self.id,
            "name": self.name,
            "container": self.container,
            "path": self.path(),
            "checksum": self.checksum,
            "size": self.size,
            "metadata": self.md_to_list(),
            "create_ts": self.create_ts,
            "modified_ts": self.modified_ts,
            "mimetype": self.mimetype or "application/octet-stream",
            "type": self.type,
            "url": self.url,
        }
        if user:
            data['can_read'] = self.user_can(user, "read")
            data['can_write'] = self.user_can(user, "write")
            data['can_edit'] = self.user_can(user, "edit")
            data['can_delete'] = self.user_can(user, "delete")
        return data

    def update(self, **kwargs):
        """Update a resource"""
        pre_state = self.mqtt_get_state()
        kwargs['modified_ts'] = datetime.now()
        pre_id = self.id

        if 'metadata' in kwargs:
            kwargs['metadata'] = meta_cdmi_to_cassandra(kwargs['metadata'])

        super(Resource, self).update(**kwargs)

        post_state = self.mqtt_get_state()

        # Update id
        if 'id' in kwargs:
            if pre_id:
                idx = IDIndex.find(pre_id)
                if idx:
                    idx.delete()
            idx = IDIndex.create(id=self.id,
                                 classname="indigo.models.resource.Resource",
                                 key=self.path())

        if pre_state['metadata'] == post_state['metadata']:
            self.mqtt_publish('update_object', pre_state, post_state)
        else:
            self.mqtt_publish('update_metadata', pre_state, post_state)
        
        self.index()

        # TODO: If we update the url we need to delete the blob

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
        cfg = get_config(None)
        keyspace = cfg.get('KEYSPACE', 'indigo')
        # The ACL we construct will replace the existing one
        # The dictionary keys are the groups id for which we have an ACE
        # We don't use aceflags yet, everything will be inherited by lower
        # sub-collections
        # acemask is set with helper (read/write - see indigo/models/acl/py)
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
                 "}}").format(gid, ident, 0, str_to_acemask(access[gid], True))
            ls_access.append(s)
        acl = "{{{}}}".format(", ".join(ls_access))
        query= ("UPDATE {}.resource SET acl = acl + {}"
                "WHERE container='{}' AND name='{}'").format(
            keyspace,
            acl,
            self.container.replace("'", "\''"),
            self.name.replace("'", "\''"))
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
                              cdmi_str_to_acemask(cdmi_ace['acemask'], True)
                             )
            ls_access.append(s)
        acl = "{{{}}}".format(", ".join(ls_access))
        query= ("UPDATE {}.resource SET acl = acl + {}"
                "WHERE container='{}' AND name='{}'").format(
            keyspace,
            acl,
            self.container.replace("'", "\''"),
            self.name.replace("'", "\''"))
        connection.execute(query)

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
