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
import time
from mimetypes import guess_type

from indigo.models.search import SearchIndex
from indigo.models.blob import Blob
from indigo.models.user import User
from indigo.models.group import Group
from indigo.models.collection import Collection
from indigo.models.resource import Resource
from indigo.models.errors import UniqueException

#Queue/Thread
from Queue import Queue
from threading import Thread

SKIP = (".pyc",)

def do_ingest(cfg, args):
    if not args.user or not args.group or not args.folder:
        print "Group, User and Folder are all required for ingesting data"
        sys.exit(1)

    # Check validity of the arguments (do user/group and folder)
    # actually exist.
    user = User.find(args.user)
    if not user:
        print u"User '{}' not found".format(args.user)
        sys.exit(1)

    group = Group.find(args.group)
    if not group:
        print u"Group '{}' not found".format(args.group)
        sys.exit(1)

    path = os.path.abspath(args.folder)
    if not os.path.exists(path):
        print u"Could not find path {}".format(path)

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
        self.local_ip = local_ip

    def create_collection(self, name, path, parent):
        d = {}
        d['path'] = path
        d['name'] = name
        d['parent'] = parent
        d['write_access']  = self.groups
        d['delete_access'] = self.groups
        d['edit_access']   = self.groups

        c = Collection.create(**d)
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
        d = {}
        d['name'] = path.split('/')[-1]
        d['file_name'] = path.split('/')[-1]

        t, _ = guess_type(path)
        _, ext = os.path.splitext(path)

        d['mimetype'] = t
        d['type'] = ext[1:].upper()
        d['size'] = os.path.getsize(path)

        #d['read_access'] = self.groups
        d['write_access']  = self.groups
        d['delete_access'] = self.groups
        d['edit_access']   = self.groups
        return d


    def start(self):
        """
        Walks the folder creating collections when it finds a folder,
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
            root_collection = Collection.create(name="Home", path="/")
        self.collection_cache["/"] = root_collection

        paths = []
        for (path, dirs, files) in os.walk(self.folder, topdown=True, followlinks = True ):
            if '/.' in path: continue # Ignore .paths
            paths.append(path)

        def name_and_parent_path(p):
            parts = p.split('/')
            return parts[-2], '/'.join(parts[:-2]) + "/"

        paths = [p[len(self.folder):] + "/" for p in paths]
        paths.sort(key=len)

        for path in paths:
            name, parent_path = name_and_parent_path(path)

            print "Processing {} with name '{}'".format(path, name)
            #print "  Parent path is {}".format(parent_path)

            if name:
                TIMER.enter('get-collection')
                parent = self.get_collection(parent_path)
                #print "  Parent collection is {}".format(parent)

                current_collection = self.get_collection(path)
                if not current_collection:
                    current_collection = self.create_collection(name, path, parent.id)
                TIMER.exit('get-collection')
            else:
                current_collection = root_collection

            # Now we can add the resources from self.folder + path
            for entry in os.listdir(self.folder + path):
                fullpath = self.folder + path + entry

                if entry.startswith("."): continue
                if entry.endswith(SKIP): continue
                if not os.path.isfile(fullpath): continue

                rdict = self.resource_for_file(fullpath)
                rdict["container"] = current_collection.id
                TIMER.enter('push')
                self.Create_Entry( rdict,
                       dict(fullpath=fullpath,
                            collection=current_collection.id ,
                            local_ip=self.local_ip,
                            path = path,
                            entry = entry
                            ) ,
                       not self.skip_import)
                TIMER.exit('push')



#### Heavy Lifting


def initializeThreading(ctr=8) :
    CreateQueue = Queue(maxsize=200)   # Create a queue on which to put the create requests
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
            Process_Create_Entry(*args)
            self.queue.task_done()


def Process_Create_Entry(rdict, context, do_load ):
    # MOSTLY the resource will not exist... so do it this way.
    try:
        resource = Resource.create(**rdict)
    except UniqueException as excpt:
        # allow_filtering is used because either we filter close to the data, or fetch eveything and filter here
        # and some of the directories in the wild are huge ( many tens of thousands of entries ).
        existing = Resource.objects.allow_filtering().filter(container=context['collection'],name=rdict['name']).all()
        for e  in existing :
            if e.name == rdict['name']:
                resource = e
                break

    if not resource.url:

        # Upload the file content as blob and blobparts!
        # TODO: Allow this file to stay where it is and reference it
        # with IP and path.

        if not do_load :
            # Specify a URL for this resource to point to the agent on this
            # machine.  It's important that the agent is configured with the
            # same root folder as the one where we import.
            url = "file://{}{}".format(context['local_ip'], context['path'] + context['entry'])
            print 'adding '+url
            resource.update(url=url)

        else:
            # Push the file into Cassandra
            with open(context['fullpath'], 'r') as f:
                blob = Blob.create_from_file(f, rdict['size'])
                if blob:
                    resource.update(url="cassandra://{}".format(blob.id))

    SearchIndex.reset(resource.id)
    SearchIndex.index(resource, ['name', 'metadata'])


class timer_counter(dict):
    # Store triples of current start time, count and total time
    def enter(self,tag):
        rcd = self.get(tag,[0 , 0 , 0.0])
        rcd[0] = time.time()
        rcd[1] += 1
        self[tag] = rcd
    def exit(self,tag):
        rcd = self.get(tag,[time.time() , 1 , 0.0])
        rcd[2] += ( time.time() - rcd[0])
    def summary(self):
        for t,v in self.items() :
            print '{0:15s}:total:{3}, count:{2}'.format(t,*v)

