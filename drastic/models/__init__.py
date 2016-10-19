"""Drastic Casandra Model
"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


import cassandra

import cassandra.cluster
from cassandra import ConsistencyLevel
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import (
    create_keyspace_network_topology,
    drop_keyspace,
    sync_table,
    create_keyspace_simple)
import time

from drastic.models.group import Group
from drastic.models.user import User
from drastic.models.node import Node
from drastic.models.collection import Collection
from drastic.models.search import SearchIndex
from drastic.models.resource import Resource
from drastic.models.blob import Blob, BlobPart
from drastic.models.activity import Activity

from drastic.log import init_log

logger = init_log('models')


def initialise(keyspace, hosts=('127.0.0.1',), strategy='SimpleStrategy',
               repl_factor=1, consistency=ConsistencyLevel.ONE):
    """Initialise Cassandra connection"""
    num_retries = 6
    retry_timeout = 1

    for retry in xrange(num_retries):
        try:
            logger.info('Connecting to Cassandra keyspace "{2}" '
                        'on "{0}" with strategy "{1}"'.format(hosts, strategy, keyspace))
            connection.setup(hosts, "system", protocol_version=3, consistency=ConsistencyLevel.ONE)

            if strategy is 'SimpleStrategy':
                create_keyspace_simple(keyspace, repl_factor, True)
            else:
                create_keyspace_network_topology(keyspace, {}, True)

            connection.setup(hosts, keyspace, protocol_version=3, consistency=ConsistencyLevel.ONE)

            break
        except cassandra.cluster.NoHostAvailable:
            logger.warning('Unable to connect to Cassandra. Retrying in {0} seconds...'.format(retry_timeout))
            time.sleep(retry_timeout)
            retry_timeout *= 2


def sync():
    """Create tables for the different models"""
    tables = (User, Node, Collection, Resource, Group, SearchIndex, Blob, BlobPart, Activity)

    for table in tables:
        logger.info('Syncing table "{0}"'.format(table.__name__))
        sync_table(table)


def destroy(keyspace):
    """Create Cassandra keyspaces"""
    logger.warning('Dropping keyspace "{0}"'.format(keyspace))
    drop_keyspace(keyspace)
#     drop_keyspace(keyspace + "_test")
