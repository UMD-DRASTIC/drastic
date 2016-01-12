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
from Queue import Queue, Empty
from cassandra.query import (
    dict_factory,
    SimpleStatement
)
import time
from cassandra.cluster import Cluster

from indigo import get_config
from indigo.models.resource import Resource
from indigo.models.collection import Collection
from indigo.models.search2 import SearchIndex2

cpt_indexed = 0
global_time = time.time()
nb_obj = -1


def index(obj):
    global cpt_indexed, global_time
    if isinstance(obj, Resource):
        SearchIndex2.index(obj, ['name', 'metadata', 'mimetype'])
        cpt_indexed += 1
    else:
        SearchIndex2.index(obj, ['name', 'metadata'])
        cpt_indexed += 1
    if cpt_indexed % 1000 == 999:
        T1 = time.time()
        if nb_obj != -1:
            percent = "({}%) ".format(int(float(cpt_indexed) / nb_obj * 100))
        else:
            percent = ""
        print '{} {}objects indexed in {} seconds = {} / sec '.format(
            cpt_indexed,
            percent,
            T1 - global_time,
            (1000.0/(T1-global_time))
        )
        global_time = T1



def do_work(q):
    """
    Pull an indexable object from the queue and process it
    This will block when the queue is empty, and once the parent issues a 'join' it will be terminated

    :param q: Queue
    :return: None
    """
    while True:
        obj = q.get(timeout=2)
        index(obj)
        q.task_done()


def do_index(cfg, args):
    global nb_obj
    cfg = get_config(None)
    keyspace = cfg.get('KEYSPACE', 'indigo')
    hosts = cfg.get('CASSANDRA_HOSTS', ('127.0.0.1', ))
    cluster = Cluster(hosts)
    session = cluster.connect(keyspace)
    session.row_factory = dict_factory


    work_queue = Queue()
    # Have to start threads before pushing stuff onto queue ....
    threads = [Thread(target=do_work, args=(work_queue,)) for k in xrange(8)]
    for t in threads:
        t.daemon = True
        t.start()

    #### Divide the search space into chunks based on the token of the primary key
    #### Will do both resource and collection in lockstep, since both are partitioned on collection
    #### so both container and objects will be despatched ( more or less ) concurrently
    ####
    hi =  (1<<63)-1
    lo =  -(1<<63)
    delta = (hi-lo)/(1<<14)         # divide into 16M chunks
    for fv in xrange(lo,hi,delta ):
        stmt = SimpleStatement('SELECT * from collection where token(container) > {} and token(container) <= {} '.format(fv,min(fv+delta,hi)))
        ctr_coll = 0
        for row in session.execute(stmt, timeout=None):
            ctr_coll += 1
            work_queue.put(Collection(**row))

        stmt = SimpleStatement('SELECT * from resource where token(container) > {} and token(container) <= {}'.format(fv,min(fv+delta,hi)))
        ctr_resc = 0
        for row in session.execute(stmt, timeout=None):
            ctr_resc += 1
            work_queue.put(Resource(**row))
        
        if ctr_resc + ctr_coll > 20000:
            break

    nb_obj = work_queue.qsize()
    print "Finishing indexing ({} objects in the queue)...".format(nb_obj)
    T0 = time.time()

    #### Wait for the queue to empty and all the work to be done..
    work_queue.join()

    print "Indexing finished"
    for t in threads:
        t.join()
    T1 = time.time() - T0
    print '{} objects processed in {} seconds = {} / sec '.format(
        nb_obj,
        T1  ,
        (nb_obj/(T1 ))
    )

