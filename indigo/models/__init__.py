from cassandra.cluster import Cluster
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import create_keyspace, sync_table

from .user import User

def initialise(keyspace, strategy="SimpleStrategy", repl_factor=1):
    connection.setup(['127.0.0.1'], "cqlengine", protocol_version=3)
    create_keyspace(keyspace, strategy, repl_factor, True)
    create_keyspace('cqlengine', strategy, repl_factor, True)


def sync():
    sync_table(User)
