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

from cStringIO import StringIO
import zipfile
from datetime import datetime
from cassandra.cqlengine import (
    columns,
    connection
)
from cassandra.query import SimpleStatement
from cassandra.cqlengine.models import Model

from indigo import get_config
from indigo.models import (
    Group,
)
from indigo.models.acl import (
    Ace,
    acl_cdmi_to_cql,
    acl_list_to_cql,
    cdmi_str_to_aceflag,
    str_to_acemask,
    cdmi_str_to_acemask,
)
from indigo.util import default_cdmi_id


static_fields = ["checksum",
                 "size",
                 "metadata",
                 "mimetype",
                 "alt_url",
                 "create_ts",
                 "modified_ts",
                 "type",
                 "acl",
                 "treepath"]


class DataObject(Model):
    """ The DataObject represents actual data objects, the tree structure
    merely references it.

    Each partition key gathers together all the data under one partition (the
    CDMI ID ) and the object properties are represented using static columns
    (one instance per partition)
    It has a similar effect to a join to a properties table, except the
    properties are stored with the rest of the partition

    This is an 'efficient' model optimised for Cassandra's quirks.

    N.B. by default Cassandra compresses its data ( using LZW ), so we get that
    for free."""
    # The 'name' of the object
    uuid = columns.Text(default=default_cdmi_id, required=True,
                        partition_key=True)
    #####################
    # These columns are the same (shared) between all entries with same id
    # (they use the static attribute , [ like an inode or a header ])
    #####################
    checksum = columns.Text(static=True)
    size = columns.BigInt(default=0, static=True)
    metadata = columns.Map(columns.Text, columns.Text, static=True)
    mimetype = columns.Text(static=True)
    alt_url = columns.Set(columns.Text, static=True)
    create_ts = columns.DateTime(default=datetime.now, static=True)
    modified_ts = columns.DateTime(default=datetime.now, static=True)
    type = columns.Text(required=False, static=True, default='UNKNOWN')
    acl = columns.Map(columns.Text, columns.UserDefinedType(Ace), static=True)
    # A general aid to integrity ...
    treepath = columns.Text(static=True, required=False)
    #####################
    # And 'clever' bit -- 'here' data, These will be the only per-record-fields
    # in the partition (i.e. object)
    # So the datastructure looks like a header , with an ordered list of blobs
    #####################
    # This is the 'clustering' key...
    sequence_number = columns.Integer(primary_key=True, partition_key=False)
    blob = columns.Blob(required=False)
    compressed = columns.Boolean(default=False)
    #####################

    @classmethod
    def append_chunk(cls, uuid, data, sequence_number, compressed=False):
        """Create a new blob for an existing data_object"""
        data_object = cls(uuid=uuid,
                          sequence_number=sequence_number,
                          blob=data,
                          compressed=compressed)
        data_object.save()
        return data_object


    def chunk_content(self):
        """
        Yields the content for the driver's URL, if any
        a chunk at a time.  The value yielded is the size of
        the chunk and the content chunk itself.
        """
        entries = DataObject.objects.filter(uuid=self.uuid)
        for entry in entries:
            if entry.compressed:
                data = StringIO(entry.blob)
                z = zipfile.ZipFile(data, 'r')
                content = z.read("data")
                data.close()
                z.close()
                yield content
            else:
                yield entry.blob


    @classmethod
    def create(cls, data, compressed=False, metadata=None, create_ts=None, acl=None):
        """data: initial data"""
        new_id = default_cdmi_id()
        now = datetime.now()
        kwargs = {
            "uuid": new_id,
            "sequence_number": 0,
            "blob": data,
            "compressed": compressed,
            "modified_ts": now
        }
        if metadata:
            kwargs['metadata'] = metadata
        if create_ts:
            kwargs['create_ts'] = create_ts
        else:
            kwargs['create_ts'] = now
        if acl:
            kwargs['acl'] = acl
        new = super(DataObject, cls).create(**kwargs)
        return new


    def create_acl(self, acl_cql):
        """Replace the static acl with the given cql string"""
        cfg = get_config(None)
        session = connection.get_session()
        keyspace = cfg.get('KEYSPACE', 'indigo')
        session.set_keyspace(keyspace)
        query = SimpleStatement(u"""UPDATE data_object SET acl = {}
            WHERE uuid=%s""".format(acl_cql))
        session.execute(query, (self.uuid,))


    def create_acl_cdmi(self, cdmi_acl):
        """""Create entry ACL from a cdmi object (list of dict)"""
        cql_string = acl_cdmi_to_cql(cdmi_acl)
        self.create_acl(cql_string)


    def create_acl_list(self, read_access, write_access):
        """Create ACL from two lists of groups id, existing ACL are replaced"""
        cql_string = acl_list_to_cql(read_access, write_access)
        self.create_acl(cql_string)


    @classmethod
    def delete_id(cls, uuid):
        """Delete all blobs for the specified uuid"""
        cfg = get_config(None)
        session = connection.get_session()
        keyspace = cfg.get('KEYSPACE', 'indigo')
        session.set_keyspace(keyspace)
        query = SimpleStatement("""DELETE FROM data_object WHERE uuid=%s""")
        session.execute(query, (uuid,))


    @classmethod
    def find(cls, uuid):
        """Find an object by uuid"""
        entries = cls.objects.filter(uuid=uuid)
        if not entries:
            return None
        else:
            return entries.first()


    def update(self, **kwargs):
        """Update a data object"""
        cfg = get_config(None)
        session = connection.get_session()
        keyspace = cfg.get('KEYSPACE', 'indigo')
        session.set_keyspace(keyspace)
        for arg in kwargs:
            # For static fields we can't use the name in the where condition
            if arg in static_fields:
                query = SimpleStatement("""UPDATE data_object SET {}=%s
                    WHERE uuid=%s""".format(arg))
                session.execute(query, (kwargs[arg], self.uuid))
            else:
                query = SimpleStatement("""UPDATE data_object SET {}=%s
                    WHERE uuid=%s and sequence_number=%s""".format(arg))
                session.execute(query, (kwargs[arg], self.container, self.sequence_number))
        return self


    def update_acl(self, acl_cql):
        """Update the static acl with the given cql string
        """
        cfg = get_config(None)
        session = connection.get_session()
        keyspace = cfg.get('KEYSPACE', 'indigo')
        session.set_keyspace(keyspace)
        query = SimpleStatement(u"""UPDATE data_object SET acl = acl + {}
            WHERE uuid=%s""".format(acl_cql))
        session.execute(query, (self.uuid,))


    def update_acl_cdmi(self, cdmi_acl):
        """"Update entry ACL from a cdmi object (list of dict)"""
        cql_string = acl_cdmi_to_cql(cdmi_acl)
        self.update_acl(cql_string)


    def update_acl_list(self, read_access, write_access):
        """Update ACL from two lists of groups id, existing ACL are replaced"""
        cql_string = acl_list_to_cql(read_access, write_access)
        self.update_acl(cql_string)

