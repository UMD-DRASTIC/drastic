# coding=utf-8
"""Ingest workflow management tool

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
import abc
import collections
import os
from collections import OrderedDict
from os.path import abspath

from cassandra.cqlengine.query import BatchQuery

from indigo.models.blob import Blob
from indigo.models.collection import Collection
from indigo.models.errors import (ResourceConflictError, NoSuchCollectionError)
from indigo.models.resource import Resource
from indigo.models.search import SearchIndex


class CollectionManager(OrderedDict):
    def __init__(self, size=1000):
        super(CollectionManager, self).__init__()
        self.maxcount = size

    def cache(self, p, c):
        #### cache, promote and flush
        self[p] = c
        while len(self) > self.maxcount: self.popitem(last=False)  # trim excess entries - FIFO
        return c

    def collection(self, path):
        path = os.path.abspath(path)
        while path[0:2] == '//': path = path[1:]
        try:
            c = self[path]
            self[path] = c  # Move back to top of FIFO ... so it stays here.
            return c
        except:
            pass

        c = Collection.find_by_path(path)
        if c:
            self.cache(path, c)
            return
        #### Ok, doesn't exist iterate up until it sticks

        p1, p2 = os.path.split(path)
        try:
            c = Collection.create(container=p1, name=p2)
            self.cache(path, c)
            return c
        except NoSuchCollectionError as e:
            self.collection(p1)  # create parent ( and implicitly grandparents &c )
            return self.collection(path)  # and now should be ok...


class writer:
    def __init__(self, args, cfg):
        pass

    @abc.abstractmethod
    def put(self):
        raise NotImplementedError


class LinkWriter(writer):
    def __init__(self, args, cfg):
        pattern = "file://{}".format(args['--local-ip'])
        self.pattern = pattern + '{path}'.format

    def put(self, path):
        path = abspath(path)
        url = self.pattern(path=path)
        p1, n1 = os.path.split(path)
        try:
            resource = Resource.create(url=url, container=p1, name=n1)
        except ResourceConflictError:
            print  url, p1, n1
            raise

        SearchIndex.reset(resource.id)
        SearchIndex.index(resource, ['name', 'metadata'])


########### function to create embedded object
class CopyWriter(writer):
    def __init__(self, args, cfg):
        self.pattern = "cassandra://{id}".format
        self.collection_mgr = CollectionManager()
        self.prefix = args['--prefix']

    def put(self, path):
        fullpath = abspath(os.path.join(self.prefix, path))
        p1, n1 = os.path.split(path)
        b = BatchQuery()
        # Create Blob
        if not os.path.exists(fullpath):
            raise ValueError(fullpath)
        succeeded = False
        for retry in range(2):  # try first time through, and if it fails then create parent and try again
            try:
                url = self.pattern(id=0)
                resource = Resource.batch(b).create(url=url, container=p1, name=n1)
                succeeded = True
            except Exception as e:
                parent = self.collection_mgr.collection(p1)
                resource = Resource.batch(b).create(url=url, container=p1, name=n1)
        if not succeeded:
            raise e

        # We should have a valid resource at this point....
        with open(fullpath, 'rb') as f:
            size = os.fstat(f.fileno()).st_size
            blob = Blob.batch(b).create_from_file(f, size)
            if blob:
                # If everything works the batch will commit here...
                url = self.pattern(id=blob.id)
                resource.update(url=url, size=size)
                b.execute()
                return resource
            else:
                return None


########### function to create embedded object remotely ( same as load, but uses CDMI instead ).
class SendWriter(writer):
    def put(self):
        raise NotImplementedError
        p1, n1 = os.path.split(path)
        # TODO: Convert this to use the CDMI PUT/POST
