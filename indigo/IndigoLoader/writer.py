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
from indigo.models.blob import BlobPart
from indigo.models.blob import Blob
from indigo.models.collection import Collection
from indigo.models.errors import (ResourceConflictError, NoSuchCollectionError)
from indigo.models.resource import Resource
from indigo.models.search import SearchIndex
from socket import error as SocketError


class CollectionManager(OrderedDict):
    def __init__(self, size=1000):
        super(CollectionManager, self).__init__()
        self.maxcount = size
        self.root = Collection.get_root_collection()


    def cache(self, p, c):
        #### cache, promote and flush
        self[p] = c
        while len(self) > self.maxcount: self.popitem(last=False)  # trim excess entries - FIFO
        return c

    def collection(self, path):
        path = os.path.abspath(path)
        while path[0:2] == '//': path = path[1:]
        if path == '/' : return self.root
        ## Do we have a cached copy?
        if path in self:
            c = self[path]
            self[path] = c  # Move back to top of FIFO ... so it stays here.
            return c
        # Otherwise find it, cache it and return it
        c = Collection.find_by_path(path)
        if c:
            return self.cache(path, c)

        #### Ok, doesn't exist iterate up until it sticks

        p1, p2 = os.path.split(path)
        try:
            rcd = dict(container=p1, name=p2)
            c = Collection.create(**rcd)
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
        self.collection_mgr = CollectionManager()

    def put(self, path):
        path = abspath(path)
        url = self.pattern(path=path)
        p1, n1 = os.path.split(path)

        try:
            parent = self.collection_mgr.collection(p1)
        except Exception as e :
            pass

        try:
            resource = Resource.create(url=url, container=p1, name=n1 )
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
        path = os.path.normpath(path)       # Tidy it all up....
        fullpath = abspath(os.path.join(self.prefix, path.lstrip('/')))
        p1, n1 = os.path.split(path)
        #b = BatchQuery()
        # Create Blob
        if not os.path.exists(fullpath):
            raise ValueError(fullpath)
        # --succeeded = False
        # We want to try to create the resource first, and if that succeeds then create the blob, and then update the
        # resource with the correct url.
        try:
            parent = self.collection_mgr.collection(p1)
        except Exception as e :
            pass
        ###
        old_resource_id = None
        try:
            resource = Resource.create(container=p1, name=n1 )
        except ResourceConflictError as e :
            ### It exists, so take note so we can tidy up later...
            resource = Resource.find_by_path(path)
            if resource.url and resource.url.startswith('cassandra://') :
                old_resource_id = resource.url[len('cassandra://'):]
        except Exception as e:
            print 'FAILED:', p1,n1
            raise e

        # We should have a valid resource at this point.... so create the blob.
        with open(fullpath, 'rb') as f:
            size = os.fstat(f.fileno()).st_size
            blob = Blob.create_from_file(f, size)
            if blob:
                # If everything works  ...
                url = self.pattern(id=blob.id)
                try :
                    resource.update(url=url, size=size)
                    ## So now tidy up the old blob
                    if old_resource_id:
                        from indigo.models.id_index import IDIndex
                        blob = Blob.find(old_resource_id)
                        if blob:
                            for k in blob.parts :

                                part_id = BlobPart.find(k)
                                if part_id : part_id.delete()
                            blob.delete()
                    ## End Tidy...      This really should be in the model
                except SocketError as e :
                    pass

                except Exception as e :
                    pass
                #try: b.execute()
                #except Exception as e :
                #    pass
                return resource
            else:
                return None


########### function to create embedded object remotely ( same as load, but uses CDMI instead ).
class SendWriter(writer):
    def put(self):
        raise NotImplementedError
        p1, n1 = os.path.split(path)
        # TODO: Convert this to use the CDMI PUT/POST
