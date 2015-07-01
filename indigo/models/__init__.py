from cassandra.cqlengine import connection
from cassandra.cqlengine.management import create_keyspace, drop_keyspace, sync_table

from indigo.models.group import Group
from indigo.models.user import User
from indigo.models.node import Node
from indigo.models.collection import Collection
from indigo.models.search import SearchIndex
from indigo.models.resource import Resource
from indigo.models.blob import Blob, BlobPart
from indigo.models.activity import Activity

def initialise(keyspace, strategy="SimpleStrategy", repl_factor=1):
    connection.setup(['127.0.0.1'], keyspace, protocol_version=3)
    create_keyspace(keyspace, strategy, repl_factor, True)

def sync():
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
    drop_keyspace(keyspace)
    drop_keyspace(keyspace + "_test")
