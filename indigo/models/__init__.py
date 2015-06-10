from cassandra.cqlengine import connection
from cassandra.cqlengine.management import create_keyspace, drop_keyspace, sync_table

from .user import User
from .node import Node
from .collection import Collection
from .resource import Resource

def initialise(keyspace, strategy="SimpleStrategy", repl_factor=1):
    connection.setup(['127.0.0.1'], keyspace, protocol_version=3)
    create_keyspace(keyspace, strategy, repl_factor, True)

def sync():
    sync_table(User)
    sync_table(Node)
    sync_table(Collection)
    sync_table(Resource)

def destroy(keyspace):
    drop_keyspace(keyspace)
    drop_keyspace(keyspace + "_test")
