import uuid
from datetime import datetime

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from indigo.models.errors import UniqueException
from indigo.util import default_uuid

class Node(Model):
    id       = columns.Text(primary_key=True, default=default_uuid)
    name     = columns.Text(required=True, index=True)
    address  = columns.Text(required=True, index=True)
    last_update = columns.DateTime()
    status   = columns.Text(required=True, default="UP")

    @classmethod
    def create(self, **kwargs):
        """
        We intercept the create call so that we can check for uniqueness
        of IP
        """
        if self.objects.filter(address=kwargs['address']).count():
            raise UniqueException("Address '{}' already in use".format(kwargs['address']))

        if not kwargs.get("last_update"):
            kwargs["last_update"] = datetime.now()

        kwargs["id"] = unicode(uuid.uuid4())
        return super(Node, self).create(**kwargs)

    @classmethod
    def find(self, name_or_address):
        node = self.objects.filter(name=name_or_address).first()
        if not node:
            node = self.objects.filter(address=name_or_address).first()
        return node

    @classmethod
    def find_by_id(self, idstring):
        return self.objects.filter(id=idstring).first()

    @classmethod
    def list(self):
        return self.objects.all()

    def status_up(self):
        self.update(status="UP")

    def status_down(self):
        self.update(status="DOWN")

    def __unicode__(self):
        return "{}/{}".format(self.name, self.address)

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'address': self.address,
            'last_update': self.last_update,
            'status': self.status
        }
