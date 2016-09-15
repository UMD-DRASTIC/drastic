"""Blob Model
"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


import hashlib
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from drastic.util import default_uuid


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
    def find(cls, id_):
        """Find an object from its id"""
        return cls.objects.filter(id=id_).first()

    def __unicode__(self):
        return unicode(self.id)


class BlobPart(Model):
    """Blob Part Model"""
    id = columns.Text(primary_key=True, default=default_uuid)
    content = columns.Bytes()
    compressed = columns.Boolean(default=False)
    blob_id = columns.Text(index=True)

    @classmethod
    def find(cls, id_):
        """Find an object from its id"""
        return cls.objects.filter(id=id_).first()

    def __unicode__(self):
        return unicode(self.id)

    def length(self):
        """Return length of the activity"""
        return len(self.content)
