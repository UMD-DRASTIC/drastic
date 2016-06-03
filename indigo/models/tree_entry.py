""" Model

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

from cassandra.cqlengine.models import Model
from cassandra.query import SimpleStatement
from cassandra.cqlengine import (
    columns,
    connection
)
from datetime import datetime

from indigo import get_config
from indigo.util import (
    default_cdmi_id,
    merge,
    meta_cdmi_to_cassandra,
)
from indigo.models import (
    Group
)
from indigo.models.acl import (
    Ace,
    acl_cdmi_to_cql,
    acl_list_to_cql,
    cdmi_str_to_aceflag,
    cdmi_str_to_acemask,
    str_to_acemask,
)

static_fields = ["container_metadata",
                 "container_uuid",
                 "container_create_ts",
                 "container_modified_ts",
                 "container_acl"]

class TreeEntry(Model):
    """TreeEntry model"""

    # Partitioned by container, clustered by name, so all files for a directory
    # are in the same bucket and share the single instance of the (static
    # container data
    container = columns.Text(partition_key=True)
    name = columns.Text(primary_key=True, partition_key=False)

    # The following set of columns are shared between all entries with the same
    # container name. i.e. it removes the need for a separate container table,
    # removes the need for extra lookups and avoids the container / objects
    # getting out of sync
    #
    # It also facilitates _some_ directory operations, e.g. removal.
    #
    # Renaming is still slow because the container and the name are primary
    # keys, so you have to create a new record and delete the old one...
    # It is suggested to use the batch system to make such an operation (more
    # or less) atomic.
    #
    container_metadata = columns.Map(columns.Text, columns.Text, static=True)
    container_uuid = columns.Text(default=default_cdmi_id, static=True)
    container_create_ts = columns.DateTime(static=True)
    container_modified_ts = columns.DateTime(static=True)
    container_acl = columns.Map(columns.Text, columns.UserDefinedType(Ace),
                                static=True)

    # This is the actual directory entry per-se, i.e. unique per name....
    # As with a conventional filesystem this is simply a reference to the 'real'
    # data where ACLs, system metadata &c are held.
    # per-record, but only for externals (see DataObject)
    metadata = columns.Map(columns.Text, columns.Text)
    create_ts = columns.DateTime(default=datetime.now)
    modified_ts = columns.DateTime()
    acl = columns.Map(columns.Text, columns.UserDefinedType(Ace))
    mimetype = columns.Text()
    # Use the url schema (file:// , cdmi:// &c ) to route the request...
    # Only cdmi:// does anything everything else results in a redirect
    url = columns.Text()
    uuid = columns.Text()


    def add_default_acl(self):
        """Add read access to all authenticated users"""
        self.create_container_acl_list(["AUTHENTICATED@"], [])


    @classmethod
    def create(cls, **kwargs):
        """Create"""
#         if "mimetype" in kwargs:
#             metadata = kwargs.get('metadata', {})
#             metadata["cdmi_mimetype"] = kwargs["mimetype"]
#             kwargs['metadata'] = meta_cdmi_to_cassandra(metadata)
#             del kwargs['mimetype']
        new = super(TreeEntry, cls).create(**kwargs)
        return new


    def create_container_acl(self, acl_cql):
        """Replace the static acl with the given cql string
        """
        cfg = get_config(None)
        session = connection.get_session()
        keyspace = cfg.get('KEYSPACE', 'indigo')
        session.set_keyspace(keyspace)
        query = SimpleStatement(u"""UPDATE tree_entry SET container_acl={} 
            WHERE container=%s""".format(acl_cql))
        session.execute(query, (self.container,))


    def create_container_acl_cdmi(self, cdmi_acl):
        """""Create static ACL from a cdmi object (list of dict)"""
        cql_string = acl_cdmi_to_cql(cdmi_acl)
        self.create_container_acl(cql_string)


    def create_container_acl_list(self, read_access, write_access):
        """""Create static ACL from  lists of group uuids"""
        cql_string = acl_list_to_cql(read_access, write_access)
        self.create_container_acl(cql_string)


    def create_entry_acl(self, acl_cql):
        """Replace the acl with the given cql string
        """
        cfg = get_config(None)
        session = connection.get_session()
        keyspace = cfg.get('KEYSPACE', 'indigo')
        session.set_keyspace(keyspace)
        query = SimpleStatement(u"""UPDATE tree_entry SET acl={} 
            WHERE container=%s and name=%s""".format(acl_cql))
        session.execute(query, (self.container, self.name,))


    def create_entry_acl_list(self, read_access, write_access):
        """""Create entry ACL from  lists of group uuids"""
        cql_string = acl_list_to_cql(read_access, write_access)
        self.create_entry_acl(cql_string)


    def create_entry_acl_cdmi(self, cdmi_acl):
        """""Create entry ACL from a cdmi object (list of dict)"""
        cql_string = acl_cdmi_to_cql(cdmi_acl)
        self.create_entry_acl(cql_string)


    def path(self):
        """Get the full path of the specific entry"""
        return merge(self.container, self.name)


    def update(self, **kwargs):
        """Update a collection"""
        cfg = get_config(None)
        session = connection.get_session()
        keyspace = cfg.get('KEYSPACE', 'indigo')
        session.set_keyspace(keyspace)
        for arg in kwargs:
            # For static fields we can't use the name in the where condition
            if arg in static_fields:
                query = SimpleStatement(u"""UPDATE tree_entry SET {}=%s
                    WHERE container=%s""".format(arg))
                session.execute(query, (kwargs[arg], self.container))
            else:
                query = SimpleStatement(u"""UPDATE tree_entry SET {}=%s
                    WHERE container=%s and name=%s""".format(arg))
                session.execute(query, (kwargs[arg], self.container, self.name))
        return self


    def update_container_acl(self, acl_cql):
        """Update the static acl with the given cql string"""
        cfg = get_config(None)
        session = connection.get_session()
        keyspace = cfg.get('KEYSPACE', 'indigo')
        session.set_keyspace(keyspace)
        query = SimpleStatement(u"""UPDATE tree_entry SET container_acl=container_acl+{} 
            WHERE container=%s""".format(acl_cql))
        session.execute(query, (self.container,))


    def update_container_acl_cdmi(self, cdmi_acl):
        """"Update static ACL from a cdmi object (list of dict)"""
        cql_string = acl_cdmi_to_cql(cdmi_acl)
        self.update_container_acl(cql_string)


    def update_container_acl_list(self, read_access, write_access):
        """"Update static ACL from  lists of group uuids"""
        cql_string = acl_list_to_cql(read_access, write_access)
        self.update_container_acl(cql_string)


    def update_entry_acl(self, acl_cql):
        """Update the acl with the given cql string"""
        cfg = get_config(None)
        session = connection.get_session()
        keyspace = cfg.get('KEYSPACE', 'indigo')
        session.set_keyspace(keyspace)
        query = SimpleStatement(u"""UPDATE tree_entry SET acl=acl+{} 
            WHERE container=%s and name=%s""".format(acl_cql))
        session.execute(query, (self.container, self.name,))


    def update_entry_acl_list(self, read_access, write_access):
        """"Update entry ACL from  lists of group uuids"""
        cql_string = acl_list_to_cql(read_access, write_access)
        self.update_entry_acl(cql_string)


    def update_entry_acl_cdmi(self, cdmi_acl):
        """"Update entry ACL from a cdmi object (list of dict)"""
        cql_string = acl_cdmi_to_cql(cdmi_acl)
        self.update_entry_acl(cql_string)

