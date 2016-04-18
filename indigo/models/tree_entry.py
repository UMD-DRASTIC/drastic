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
    cdmi_str_to_aceflag,
    cdmi_str_to_acemask,
    str_to_acemask,
)

static_fields = ["container_metadata",
                 "container_id",
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
    container_id = columns.Text(default=default_cdmi_id, static=True)
    container_create_ts = columns.DateTime(static=True)
    container_modified_ts = columns.DateTime(static=True)
    container_acl = columns.Map(columns.Text, columns.UserDefinedType(Ace),
                                static=True)

    # This is the actual directory entry per-se, i.e. unique per name....
    # As with a conventional filesystem this is simply a reference to the 'real'
    # data where ACLs, system metadata &c are held.
    # per-record, but only for externals (see RealObject)
    metadata = columns.Map(columns.Text, columns.Text)
    # Use the url schema (file:// , cdmi:// &c ) to route the request...
    # Only cdmi:// does anything everything else results in a redirect
    url = columns.Text()
    id = columns.Text()


    def add_default_acl(self):
        """Add read access to all authenticated users"""
        self.update_acl(["AUTHENTICATED@"], [])


    @classmethod
    def create(cls, **kwargs):
        """Create"""
        if "mimetype" in kwargs:
            metadata = kwargs.get('metadata', {})
            metadata["cdmi_mimetype"] = kwargs["mimetype"]
            kwargs['metadata'] = meta_cdmi_to_cassandra(metadata)
            del kwargs['mimetype']
        new = super(TreeEntry, cls).create(**kwargs)
        return new


    def create_acl(self, read_access, write_access):
        """""Create ACL from  lists of group uuids"""
        self.update_acl(read_access, write_access)


    def path(self):
        """Get the full path of the specific entry"""
        return merge(self.container, self.name)


    def update(self, **kwargs):
        """Update a collection"""
        cfg = get_config(None)
        session = connection.get_session()
        keyspace = cfg.get('KEYSPACE', 'indigo')
        session.set_keyspace(keyspace)

        if "mimetype" in kwargs:
            metadata = kwargs.get('metadata', {})
            metadata["cdmi_mimetype"] = kwargs["mimetype"]
            kwargs['metadata'] = meta_cdmi_to_cassandra(metadata)
            del kwargs['mimetype']

        for arg in kwargs:
            # For static fields we can't use the name in the where condition
            if arg in static_fields:
                query = SimpleStatement("""UPDATE tree_entry SET {}=%s
                    WHERE container=%s""".format(arg))
                session.execute(query, (kwargs[arg], self.container))
            else:
                query = SimpleStatement("""UPDATE tree_entry SET {}=%s
                    WHERE container=%s and name=%s""".format(arg))
                session.execute(query, (kwargs[arg], self.container, self.name))
        return self


    def update_cdmi_acl(self, cdmi_acl):
        """Update acl with the metadata acl passed with a CDMI request"""
        cfg = get_config(None)
        session = connection.get_session()
        keyspace = cfg.get('KEYSPACE', 'indigo')
        session.set_keyspace(keyspace)
        ls_access = []
        for cdmi_ace in cdmi_acl:
            if 'identifier' in cdmi_ace:
                gid = cdmi_ace['identifier']
            else:
                # Wrong syntax for the ace
                continue
            g = Group.find(gid)
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
        query = """UPDATE tree_entry SET container_acl={}
            WHERE container='{}'""".format(acl,
                                           self.container.replace("'", "\''"))
        session.execute(query)


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
        session = connection.get_session()
        keyspace = cfg.get('KEYSPACE', 'indigo')
        session.set_keyspace(keyspace)
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

        query = SimpleStatement("""UPDATE tree_entry SET container_acl={} 
            WHERE container=%s""".format(acl))
        session.execute(query, (self.container,))

