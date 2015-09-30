"""Ingest script

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

import os
import sys
from os.path import expanduser
import time
from mimetypes import guess_type
from cassandra.cqlengine.query import BatchQuery
#Queue/Thread
from Queue import Queue
from threading import Thread
import logging

from indigo.models.search import SearchIndex
from indigo.models.blob import Blob
from indigo.models.user import User
from indigo.models.group import Group
from indigo.models.collection import Collection
from indigo.models.resource import Resource
from indigo.models.errors import (
    CollectionConflictError,
    ResourceConflictError
)
from indigo.util import split

SKIP = (".pyc",)

def create_logger():
    logger = logging.getLogger('ingest')
    logger.setLevel(logging.INFO)
    log_dir = expanduser('~/.indigo')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    ch = logging.FileHandler('{}/ingest.log'.format(log_dir))
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s -%(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

logger = create_logger()

def do_ingest(cfg, args):
    if not args.user or not args.group or not args.folder:
        msg = "Group, User and Folder are all required for ingesting data"
        logger.error(msg)
        print msg
        sys.exit(1)

    # Check validity of the arguments (do user/group and folder)
    # actually exist.
    user = User.find(args.user)
    if not user:
        msg = u"User '{}' not found".format(args.user)
        logger.error(msg)
        print msg
        sys.exit(1)

    group = Group.find(args.group)
    if not group:
        msg = u"Group '{}' not found".format(args.group)
        logger.error(msg)
        print msg
        sys.exit(1)

    path = os.path.abspath(args.folder)
    if not os.path.exists(path):
        msg = u"Could not find path {}".format(path)
        logger.error(msg)
        print msg

    local_ip = args.local_ip
    skip_import = args.no_import

    ingester = Ingester(user, group, path, local_ip, skip_import)
    ingester.start()


class Ingester(object):

    def __init__(self, user, group, folder, local_ip='127.0.0.1', skip_import=False):
        self.groups = [group.id]
        self.user = user
        self.folder = folder
        self.collection_cache = {}
        self.skip_import = skip_import
        if local_ip:
            self.local_ip = local_ip
        else:
            self.local_ip = '127.0.0.1'
        self.queue = None

    def create_collection(self, parent_path, name, path):
        try:
            d = {'container' : parent_path,
                 'name' : name,
                 'write_access' : self.groups,
                 'delete_access' : self.groups,
                 'edit_access' : self.groups,
                }
            c = Collection.create(**d)
        except CollectionConflictError:
            # Collection already exists
            c = Collection.find_by_path(path)
        self.collection_cache[path] = c
        return c

    def get_collection(self, path):
        c = self.collection_cache.get(path, None)
        if c:
            return c

        c = Collection.find_by_path(path)
        if c:
            self.collection_cache[path] = c
            return c

        return None

    def resource_for_file(self, path):
        t, _ = guess_type(path)
        _, name  = split(path)
        _, ext = os.path.splitext(path)
        return {"mimetype" : t,
                "size" : os.path.getsize(path),
                # "read_access" : self.groups,
                "write_access" : self.groups,
                "delete_access" : self.groups,
                "edit_access" : self.groups,
                "file_name" :  name,
                "name" : name ,
                "type" : ext[1:].upper(),
               }

    def start(self):
        """Walks the folder creating collections when it finds a folder,
        and resources when it finds a file. This is done sequentially
        so multiple copies of this program can be run in parallel (each
        with a different root folder).
        """
        self.queue = initializeThreading()
        self.doWork()
        terminateThreading(self.queue)

    def Create_Entry(self,rdict, context, do_load):
        self.queue.put( (rdict.copy(),context.copy(),do_load) )
        return

    def doWork(self):
        TIMER = timer_counter()

        root_collection = Collection.get_root_collection()
        if not root_collection:
            root_collection = Collection.create_root()
        self.collection_cache["/"] = root_collection

        for (path, dirs, files) in os.walk(self.folder, topdown=True, followlinks = True ):
            if '/.' in path: continue # Ignore .paths
            path = path.replace(self.folder, '')

            parent_path, name = split(path)
            logger.info("Processing {} - '{}'".format(path, name))

            if name:
                TIMER.enter('get-collection')
                parent = self.get_collection(parent_path)

                current_collection = self.get_collection(path)
                if not current_collection:
                    current_collection = self.create_collection(parent.path(),
                                                                name,
                                                                path)
                TIMER.exit('get-collection')
            else:
                current_collection = root_collection

            # Now we can add the resources from self.folder + path
            for entry in files:
                fullpath = self.folder + path + '/' + entry
                if entry.startswith("."): continue
                if entry.endswith(SKIP): continue
                if not os.path.isfile(fullpath): continue

                rdict = self.resource_for_file(fullpath)
                rdict["container"] = current_collection.path()
                TIMER.enter('push')
                self.Create_Entry(rdict,
                                  {"fullpath" : fullpath,
                                   "container" : current_collection.path(),
                                   "local_ip" : self.local_ip,
                                   "path" : path,
                                   "entry" : entry
                                  },
                                  not self.skip_import)
                TIMER.exit('push')

        TIMER.summary()


#### Heavy Lifting


def initializeThreading(ctr=16) :
    CreateQueue = Queue(maxsize=900)   # Create a queue on which to put the create requests
    for k in range( abs(ctr) ) :
        t = ThreadClass(CreateQueue)    # Create a number of threads
        t.setDaemon(True)               # Stops us finishing until all the threads gae exited
        t.start()                       # let it rip
    return CreateQueue

def terminateThreading(queue):
    from time import time
    T0 = time()
    queue.join()
    print time() - T0,'seconds to wrap up outstanding'



class ThreadClass(Thread) :

    def __init__(self,q) :
        Thread.__init__(self)
        self.queue = q

    def run(self) :
        while True :
            args = self.queue.get()
            self.Process_Create_Entry(*args)
            self.queue.task_done()


# def Process_Create_Entry(rdict, context, do_load):
#     # MOSTLY the resource will not exist... so do it this way.
#     try:
#         resource = Resource.create(**rdict)
#     except ResourceConflictError as excpt:
#         # allow_filtering is used because either we filter close to the data, or fetch eveything and filter here
#         # and some of the directories in the wild are huge ( many tens of thousands of entries ).
#         existing = Resource.objects.allow_filtering().filter(container=context['container'],name=rdict['name']).all()
#         resource = None
#         for e  in existing :
#             if e.name == rdict['name']:
#                 resource = e
#                 break
#         if resource is None:
#             # ResourceConflict raised but resource not found
#             print "ERROR - Process_Create_Entry - {}/{} not created but not found".format(context['container'], rdict['name'])
# 
#     # If resource.url for file changed we modified the field
# 
#     # Upload the file content as blob and blobparts!
#     # TODO: Allow this file to stay where it is and reference it
#     # with IP and path.
# 
#     if not do_load :
#         # Specify a URL for this resource to point to the agent on this
#         # machine.  It's important that the agent is configured with the
#         # same root folder as the one where we import.
#         new_url = "file://{}{}/{}".format(context['local_ip'],
#                                           context['path'],
#                                           context['entry'])
#         if new_url != resource.url:
#             print 'adding {}'.format(new_url)
#             resource.update(url=new_url)
# 
#     else:
#         if not resource.url:
#             # Push the file into Cassandra
#             with open(context['fullpath'], 'r') as f:
#                 blob = Blob.create_from_file(f, rdict['size'])
#                 if blob:
#                     resource.update(url="cassandra://{}".format(blob.id))
# 
#     SearchIndex.reset(resource.id)
#     SearchIndex.index(resource, ['name', 'metadata'])



    def Process_Create_Entry_work(self, rdict, context, do_load ):
        b = BatchQuery()
        # MOSTLY the resource will not exist... so  start by calculating the URL and trying to insert the entire record....
        if not do_load :
            url = "file://{}{}/{}".format(context['local_ip'],
                                          context['path'],
                                          context['entry'])
        else :
            with open(context['fullpath'], 'r') as f:
                blob = Blob.create_from_file(f, rdict['size'])
                if blob:
                    url="cassandra://{}".format(blob.id)
                else:
                    return None
    
        try:
            # OK -- try to insert ( create ) the record...
            T1 = time.time()
            resource = Resource.batch(b).create(url=url,**rdict)
            msg = 'Resource {} created --> {}'.format(resource.name,
                                                      time.time() - T1)
            logger.info(msg)
        except ResourceConflictError as excpt:
            # If the create fails, the record already exists... so retrieve it...
            T1 = time.time()
            resource = Resource.objects().get(container=context['collection'],name=rdict['name'])
            msg = "{} ::: Fetch Object -> {}".format(resource.name, time.time() - T1)
            logger.info(msg)
    
        # if the url is not correct then update
        # TODO:  if the url is a block set that is stored internally then reduce it's count so that it can be garbage collected.
        T3 = None
        if resource.url != url :
            T2 = time.time()
            # if url.startswith('cassandra://') : tidy up the stored block count...
            resource.batch(b).update(url=url)
            T3 = time.time()
            msg = "{} ::: update -> {}".format(resource.name, T3 - T2)
            logger.info(msg)
    
        T1 = time.time()
        SearchIndex.reset(resource.id)
        SearchIndex.index(resource, ['name' ,'metadata'])
        
        msg = "Index Management -> {}".format(time.time() - T1)
        logger.info(msg)
        b.execute()
    
    
    def Process_Create_Entry(self, rdict, context, do_load ):
        retries = 4
        while retries > 0 :
            try: 
                return self.Process_Create_Entry_work(rdict, context, do_load )
            except Exception as e :
                logger.error("Problem creating entry: {}/{}, retry number: {} - {}".format(rdict['name'],
                                                                                           rdict['container'],
                                                                                           retries,
                                                                                           e))
                retries -= 1
        raise


class timer_counter(dict):
    # Store triples of current start time, count and total time
    trigger = 1

    def enter(self, tag):
        rcd = self.get(tag,[0 , 0 , 0.0])
        rcd[0] = time.time()
        rcd[1] += 1
        self[tag] = rcd

    def exit(self, tag):
        rcd = self.get(tag,[time.time() , 1 , 0.0])
        rcd[2] += ( time.time() - rcd[0])

    def summary(self):
        for t,v in self.items():
            logger.info('{0:15s}:total:{3}, count:{2}'.format(t,*v))

