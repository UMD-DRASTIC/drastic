import uuid
from datetime import datetime

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from indigo.models.errors import UniqueException
from indigo.util import default_id

class Resource(Model):
    id        = columns.Text(primary_key=True, default=default_id)
    name      = columns.Text(required=True, index=True)
    container = columns.Text(required=True, index=True)

    @classmethod
    def find(self, name):
        return self.objects.filter(name=name).first()

    @classmethod
    def find_by_id(self, idstring):
        return self.objects.filter(id=idstring).first()

    def __unicode__(self):
        return unicode(self.name)

    def to_dict(self):
        return  {
        }
