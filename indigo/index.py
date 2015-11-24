"""Index script

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

from threading import Thread
import os
import sys
from Queue import Queue
from cassandra.cqlengine import connection
from cassandra.query import SimpleStatement
from cassandra.cqlengine.functions import Token
import time
from cassandra.cluster import Cluster


from indigo import get_config
from indigo.models.resource import Resource
from indigo.models.collection import Collection
from indigo.models.search import SearchIndex
from indigo.models.id_index import IDIndex
from indigo.models import initialise
from indigo.models.blob import Blob
from indigo.models.errors import (
    ResourceConflictError,
    NoSuchCollectionError
)
from indigo.models.search import SearchIndex
from indigo.util import default_cdmi_id

fp1 = open('null_id.txt','w')
fp2 = open('missing_idindex.txt','w')


def Start(q):
    while True:
        obj = q.get()
        cnt = index_obj(obj)
        q.task_done()


def index_obj(obj):
    T0 = time.time()
    if not obj.id:
        fp1.write("{}\n".format(str(dict(obj))))
        fp1.flush()
        # If the object doesn't have an id we regenerate one
        obj.update(id=default_cdmi_id())
    else:
        # Check that the id is present in IDIndex
        idx = IDIndex.find(obj.id)
        if not idx:
            if isinstance(obj, Resource):
                class_name = "indigo.models.resource.Resource"
            else:
                class_name = "indigo.models.collection.Collection"
            fp2.write("{}\n".format(str(dict(obj))))
            fp2.flush()
            idx = IDIndex.create(id=obj.id,
                                 classname=class_name,
                                 key=obj.path())
        # Reindex the object
        obj.index()


def do_index(cfg, args):
    cfg = get_config(None)
    keyspace = cfg.get('KEYSPACE', 'indigo')
    hosts = cfg.get('CASSANDRA_HOSTS', ('127.0.0.1', ))
    cluster = Cluster(hosts)
    session = cluster.connect(keyspace)
    
    q = Queue(100)
    threads = [Thread(target=Start, args=(q,)) for k in xrange(10)]
    for t in threads:
        t.setDaemon(True)
        t.start()

    T0 = time.time()
    ctr = 0

    stmt = SimpleStatement('SELECT id,container,name from collection')
    for id, container, name in session.execute(stmt):
        ctr += 1
        collection = Collection.objects.filter(container=container, name=name).first()
        if collection:
            q.put(collection)
        if ctr % 1000 == 999:
            T1 = time.time()
            print '{} directories processed in {} seconds = {} / sec '.format(
                ctr,
                T1 - T0,
                (1000.0/(T1-T0))
            )
            T0 = T1

    T0 = time.time()
    ctr = 0
    stmt = SimpleStatement('SELECT id,container,name from resource ')
    for id, container, name in session.execute(stmt):
        ctr += 1
        resource = Resource.objects.filter(container=container, name=name).first()
        if resource:
            q.put(resource)
        if ctr % 1000 == 999:
            T1 = time.time()
            print '{} resources processed in {} seconds = {} / sec '.format(
                ctr,
                T1 - T0,
                (1000.0/(T1-T0))
            )
            T0 = T1
    q.join()      # block until all tasks are done
