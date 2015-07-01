"""
"""
import uuid
from datetime import datetime

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from indigo.models.errors import UniqueException
from indigo.util import default_id


class Blob(Model):
    id      = columns.Text(primary_key=True, default=default_id)
    parts   = columns.List(columns.Text, default=[], index=True)
    size    = columns.Integer(default=0)
    hash    = columns.Text(default="")

    @classmethod
    def find(cls, id):
        return cls.objects.filter(id=id).first()

    def __unicode__(self):
        return unicode(self.id)


class BlobPart(Model):
    id       = columns.Text(primary_key=True, default=default_id)
    content  = columns.Bytes()
    blob_id     = columns.Text(index=True)

    def length(self):
        return len(self.content)

    @classmethod
    def find(cls, id):
        return cls.objects.filter(id=id).first()

    def __unicode__(self):
        return unicode(self.id)

