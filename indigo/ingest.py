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
import logging

import os
import sys
import time
from threading import Thread
from mimetypes import guess_type
from Queue import Queue

from cassandra.cqlengine.query import BatchQuery

from indigo.models.search import SearchIndex
from indigo.models import User
from indigo.models import Group
#from indigo.models.collection import Collection
#from indigo.models.resource import Resource
from indigo.models.errors import (
    CollectionConflictError,
    ResourceConflictError
)
from indigo.util import split
import log

logger = log.init_log('ingest')
SKIP = (".pyc",)


def decode_str(s):
    try:
        return s.decode('utf8')
    except UnicodeDecodeError:
        try:
            return s.decode('iso8859-1')
        except UnicodeDecodeError:
            s_ignore = s.decode('utf8', 'ignore')
            logger.error("Unicode decode error for {}, had to ignore character".format(s_ignore))
            return s_ignore


logger.setLevel(logging.WARNING)


# noinspection PyUnusedLocal
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

    include_pattern = args.include

    local_ip = args.local_ip
    skip_import = args.no_import

    ingester = Ingester(user, group, path, local_ip=local_ip, skip_import=skip_import, include_pattern=include_pattern)
    ingester.start()


class Ingester(object):

    def __init__(self, user, group, folder, local_ip='127.0.0.1', skip_import=False, include_pattern=None):
        self.groups = [group.uuid]
        self.user = user
        self.folder = folder
        self.collection_cache = {}
        self.skip_import = skip_import
        if local_ip:
            self.local_ip = local_ip
        else:
            self.local_ip = '127.0.0.1'
        if include_pattern:
            self.include_pattern = include_pattern.lower()
        else:
            self.include_pattern = None
        self.queue = None

    def create_collection(self, parent_path, name, path):
        try:
            d = {'container': parent_path,
                 'name': name,
                 'write_access': self.groups,
                 'delete_access': self.groups,
                 'edit_access': self.groups,
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
        _, name = split(path)
        _, ext = os.path.splitext(path)
        return {"mimetype": t,
                "size": os.path.getsize(path),
                # "read_access": self.groups,
                "write_access": self.groups,
                "delete_access": self.groups,
                "edit_access": self.groups,
                "name": name,
                "type": ext[1:].upper(),
                }

    def start(self):
        """Walks the folder creating collections when it finds a folder,
        and resources when it finds a file. This is done sequentially
        so multiple copies of this program can be run in parallel (each
        with a different root folder).
        """
        self.queue = initialize_threading()
        self.do_work()
        terminate_threading(self.queue)

    def create_entry(self, rdict, context, do_load):
        self.queue.put((rdict.copy(), context.copy(), do_load))
        return

    def do_work(self):
        # if self.include_pattern:
        #     include_pattern = self.include_pattern.lower()
        #     matcher = lambda x: include_pattern in x.lower()
        # else:
        #     matcher = lambda x: True

        timer = TimerCounter()

        root_collection = Collection.get_root_collection()

        if not root_collection:
            root_collection = Collection.create_root()

        self.collection_cache["/"] = root_collection

        t0 = time.time()
        if self.include_pattern:
            print self.include_pattern
            start_dir = None
            for (path, dirs, files) in os.walk(self.folder, topdown=True, followlinks=True):
                for k in dirs:
                    if self.include_pattern.lower() in k.lower():
                        start_dir = os.path.join(path, k)
                        break
                if start_dir:
                    break
        else:
            start_dir = self.folder

        if start_dir:
            print "STARTING AT ", start_dir
        else:
            print "No start dir matching {0} found ... giving up ".format(self.include_pattern)
            return None

        for (path, dirs, files) in os.walk(start_dir, topdown=True, followlinks=True):
            if '/.' in path:
                continue  # Ignore .paths

            path = path.replace(self.folder, '')

            parent_path, name = split(path)

            t1 = time.time()
            msg = "Processing {0} - '{1}' Previous={2:02f}s , this={3} files".format(path, name, t1 - t0, len(files))
            t0 = t1
            logger.info("Processing {} - '{}'".format(path, name))
            print (msg)

            if name:
                timer.enter('get-collection')
                # parent = self.get_collection(parent_path)

                current_collection = self.get_collection(path)
                if not current_collection:
                    current_collection = self.create_collection(parent_path,  # parent.path(),
                                                                name,
                                                                path)
                timer.exit('get-collection')
            else:
                current_collection = root_collection

            # Now we can add the resources from self.folder + path
            for entry in files:
                fullpath = self.folder + path + '/' + entry
                if entry.startswith("."):
                    continue

                if entry.endswith(SKIP):
                    continue

                if not os.path.isfile(fullpath):
                    continue

                rdict = self.resource_for_file(fullpath)
                rdict["container"] = current_collection.path()
                timer.enter('push')
                self.create_entry(rdict,
                                  {"fullpath": fullpath,
                                   "container": current_collection.path(),
                                   "local_ip": self.local_ip,
                                   "path": path,
                                   "entry": entry
                                   },
                                  not self.skip_import)
                timer.exit('push')

        timer.summary()


# Heavy Lifting
def initialize_threading(ctr=8):
    createqueue = Queue(maxsize=900)   # Create a queue on which to put the create requests
    for k in range(abs(ctr)):
        t = ThreadClass(createqueue)    # Create a number of threads
        t.setDaemon(True)               # Stops us finishing until all the threads gae exited
        t.start()                       # let it rip
    return createqueue


def terminate_threading(queue):
    from time import time
    t0 = time()
    queue.join()
    print time() - t0, 'seconds to wrap up outstanding'


class ThreadClass(Thread):

    def __init__(self, q):
        Thread.__init__(self)
        self.queue = q

    def run(self):
        while True:
            args = self.queue.get()
            self.process_create_entry(*args)
            self.queue.task_done()

    def process_create_entry_work(self, rdict, context, do_load):
        b = BatchQuery()
        # MOSTLY the resource will not exist... so start by calculating the URL and trying to insert the entire record..
        if not do_load:
            url = "file://{}{}/{}".format(context['local_ip'],
                                          context['path'],
                                          context['entry'])
        else:
            with open(context['fullpath'], 'r') as f:
                blob = Blob.create_from_file(f, rdict['size'])
                if blob:
                    url = "cassandra://{}".format(blob.id)
                else:
                    return None

        try:
            # OK -- try to insert ( create ) the record...
            t1 = time.time()
            resource = Resource.batch(b).create(url=url, **rdict)
            msg = 'Resource {} created --> {}'.format(resource.get_name(),
                                                      time.time() - t1)
            logger.info(msg)
        except ResourceConflictError:
            # If the create fails, the record already exists... so retrieve it...
            t1 = time.time()
            resource = Resource.objects().get(container=context['collection'], name=rdict['name'])
            msg = "{} ::: Fetch Object -> {}".format(resource.get_name(), time.time() - t1)
            logger.info(msg)

        # if the url is not correct then update
        # TODO: if the url is a block set that is stored internally then reduce its count so that it can be GC'd.
        # t3 = None
        if resource.url != url:
            t2 = time.time()
            # if url.startswith('cassandra://') : tidy up the stored block count...
            resource.batch(b).update(url=url)
            t3 = time.time()
            msg = "{} ::: update -> {}".format(resource.get_name(), t3 - t2)
            logger.info(msg)

        # t1 = time.time()
        SearchIndex.reset(resource.uuid)
        SearchIndex.index(resource, ['name', 'metadata'])

        # msg = "Index Management -> {}".format(time.time() - t1)
        # logger.info(msg)
        b.execute()

    def process_create_entry(self, rdict, context, do_load):
        retries = 4
        while retries > 0:
            try:
                return self.process_create_entry_work(rdict, context, do_load)
            except Exception as e:
                logger.error("Problem creating entry: {}/{}, retry number: {} - {}".format(rdict['name'],
                                                                                           rdict['container'],
                                                                                           retries,
                                                                                           e))
                retries -= 1
        raise


# noinspection PyTypeChecker
class TimerCounter(dict):
    # Store triples of current start time, count and total time
    trigger = 1

    def enter(self, tag):
        rcd = self.get(tag, [0, 0, 0.0])
        rcd[0] = time.time()
        rcd[1] += 1
        self[tag] = rcd

    def exit(self, tag):
        rcd = self.get(tag, [time.time(), 1, 0.0])
        rcd[2] += (time.time() - rcd[0])

    def summary(self):
        for t, v in self.items():
            logger.info('{0:15s}:total:{3}, count:{2}'.format(t, *v))
