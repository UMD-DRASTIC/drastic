"""Indigo Casandra Model

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

from cassandra.cqlengine import connection
from cassandra.cqlengine.management import (
    create_keyspace,
    drop_keyspace,
    sync_table
)

from indigo.models.group import Group
from indigo.models.user import User
from indigo.models.node import Node
from indigo.models.collection import Collection
from indigo.models.search import SearchIndex
from indigo.models.resource import Resource
from indigo.models.blob import (
    Blob,
    BlobPart
)
from indigo.models.activity import Activity


def initialise(keyspace, hosts=['127.0.0.1'], strategy="SimpleStrategy",
               repl_factor=1):
    """Initialise Cassandra connection"""
    connection.setup(hosts, keyspace, protocol_version=3)
    create_keyspace(keyspace, strategy, repl_factor, True)


def sync():
    """Create tables for the different models"""
    sync_table(User)
    sync_table(Node)
    sync_table(Collection)
    sync_table(Resource)
    sync_table(Group)
    sync_table(SearchIndex)
    sync_table(Blob)
    sync_table(BlobPart)
    sync_table(Activity)


def destroy(keyspace):
    """Create Cassandra keyspaces"""
    drop_keyspace(keyspace)
#     drop_keyspace(keyspace + "_test")
