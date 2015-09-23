"""Blob Model

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


import hashlib
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from indigo.util import default_uuid


class Blob(Model):
    """Blob Model"""
    id = columns.Text(primary_key=True, default=default_uuid)
    parts = columns.List(columns.Text, default=[], index=True)
    size = columns.Integer(default=0)
    hash = columns.Text(default="")

    @classmethod
    def create_from_file(cls, fileobj, size):
        """Create an object from an opened file"""
        blob = cls.create(size=size)
        hasher = hashlib.sha256()

        chunk_size = 1024 * 1024 * 1
        parts = []
        while True:
            data = fileobj.read(chunk_size)
            if not data:
                break
            part = BlobPart.create(content=data, blob_id=blob.id)
            parts.append(part.id)

            hasher.update(data)

        blob.update(parts=parts, hash=hasher.hexdigest())
        return blob

    @classmethod
    def find(cls, id):
        """Find an object from its id"""
        return cls.objects.filter(id=id).first()

    def __unicode__(self):
        return unicode(self.id)


class BlobPart(Model):
    """Blob Part Model"""
    id = columns.Text(primary_key=True, default=default_uuid)
    content = columns.Bytes()
    compressed = columns.Boolean(default=False)
    blob_id = columns.Text(index=True)

    @classmethod
    def find(cls, id):
        """Find an object from its id"""
        return cls.objects.filter(id=id).first()

    def __unicode__(self):
        return unicode(self.id)

    def length(self):
        """Return length of the activity"""
        return len(self.content)
