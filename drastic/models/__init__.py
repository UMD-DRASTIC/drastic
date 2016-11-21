"""Drastic Casandra Model

"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


import cassandra

import cassandra.cluster
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.cqlengine import connection
from cassandra.query import dict_factory
from cassandra.cqlengine.management import (
    create_keyspace_network_topology,
    drop_keyspace,
    sync_table,
    create_keyspace_simple)
import time

from drastic.models.group import Group
from drastic.models.user import User
from drastic.models.tree_entry import TreeEntry
from drastic.models.collection import Collection
from drastic.models.data_object import DataObject
from drastic.models.listener_log import ListenerLog
from drastic.models.resource import Resource
from drastic.models.search import SearchIndex
from drastic.models.id_search import IDSearch
from drastic.models.acl import Ace
from drastic.models.notification import Notification

from drastic.log import init_log

logger = init_log('models')


def connect(keyspace="drastic", hosts=('127.0.0.1',), consistency=ConsistencyLevel.LOCAL_ONE,
            default_read_timeout=60):
    """Initialise Cassandra connection"""
    num_retries = 6
    retry_timeout = 1

    for retry in xrange(num_retries):
        try:
            logger.info('Connecting to Cassandra keyspace "{1}" '
                        'on hosts "{0}"'.format(hosts, keyspace))
            mycluster = Cluster(hosts, protocol_version=4,
                                connect_timeout=5)
            session = mycluster.connect(keyspace=keyspace)
            session.row_factory = dict_factory
            session.default_consistency_level = consistency
            session.default_timeout = default_read_timeout
            connection.set_session(session)
            break
        except cassandra.cluster.NoHostAvailable:
            logger.warning(
                'Unable to connect to Cassandra. Retrying in {0} seconds...'.format(retry_timeout))
            time.sleep(retry_timeout)
            retry_timeout *= 2


def create_keyspace(keyspace="drastic", hosts=('127.0.0.1',), strategy='SimpleStrategy',
                    repl_factor=1):
    """Initialise Cassandra keyspace"""
    num_retries = 6
    retry_timeout = 1

    for retry in xrange(num_retries):
        try:
            logger.info('Creating Cassandra keyspace "{2}" '
                        'on hosts "{0}" with strategy "{1}" and replication factor "{3}"'
                        .format(hosts, strategy, keyspace, repl_factor))
            mycluster = Cluster(hosts, protocol_version=4,
                                connect_timeout=5)
            session = mycluster.connect()
            session.row_factory = dict_factory
            session.default_consistency_level = ConsistencyLevel.ALL,
            connection.set_session(session)

            if strategy is 'SimpleStrategy':
                create_keyspace_simple(keyspace, repl_factor, True)
            else:
                create_keyspace_network_topology(keyspace, {}, True)

            break
        except cassandra.cluster.NoHostAvailable:
            logger.warning(
                'Unable to connect to Cassandra. Retrying in {0} seconds...'.format(retry_timeout))
            time.sleep(retry_timeout)
            retry_timeout *= 2


def sync():
    """Create tables for the different models"""
    tables = (User, Group, SearchIndex, IDSearch, TreeEntry, DataObject,
              Notification, ListenerLog)

    for table in tables:
        logger.info('Syncing table "{0}"'.format(table.__name__))
        sync_table(table)


def destroy(keyspace):
    """Create Cassandra keyspaces"""
    logger.warning('Dropping keyspace "{0}"'.format(keyspace))
    drop_keyspace(keyspace)
#     drop_keyspace(keyspace + "_test")
