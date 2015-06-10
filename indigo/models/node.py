import uuid
from datetime import datetime

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

class Node(Model):
    id       = columns.UUID(primary_key=True, default=uuid.uuid4)
    name     = columns.Text(required=True, index=True)
    address  = columns.Text(required=True)

    def __unicode__(self):
        return unicode(self.name)

    def to_dict(self):
        return {
            'name': self.name,
            'address': self.address,
        }
