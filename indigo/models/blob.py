"""
"""
import uuid
import hashlib
from datetime import datetime

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from indigo.models.errors import UniqueException
from indigo.util import default_uuid


class Blob(Model):
    id      = columns.Text(primary_key=True, default=default_uuid)
    parts   = columns.List(columns.Text, default=[], index=True)
    size    = columns.Integer(default=0)
    hash    = columns.Text(default="")

    @classmethod
    def find(cls, id):
        return cls.objects.filter(id=id).first()

    @classmethod
    def create_from_file(cls, fileobj, size):
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


    def __unicode__(self):
        return unicode(self.id)


class BlobPart(Model):
    id       = columns.Text(primary_key=True, default=default_uuid)
    content  = columns.Bytes()
    compressed = columns.Boolean(default=False)
    blob_id     = columns.Text(index=True)

    def length(self):
        return len(self.content)

    @classmethod
    def find(cls, id):
        return cls.objects.filter(id=id).first()

    def __unicode__(self):
        return unicode(self.id)

