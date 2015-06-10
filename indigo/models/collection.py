import uuid
from datetime import datetime

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from indigo.models.errors import UniqueException

class Collection(Model):
    id       = columns.UUID(primary_key=True, default=uuid.uuid4)
    name     = columns.Text(required=True, index=True)

    # ID of the parent collection, if "" then is the root element
    parent   = columns.Text(required=True, index=True)
    path     = columns.Text(required=True)

    def get_child_collections(self):
        return Collection.objects.filter(parent=self.id).all()

    def get_child_resources(self):
        from indigo.models import Resource
        return Resource.objects.filter(container=self.id).all()

    def get_child_resource_count(self):
        from indigo.models import Resource
        return Resource.objects.filter(container=self.id).count()

    def get_parent_collection(self):
        return Collection.find_by_id(self.parent)

    def get_root_collection(self):
        return Collection.objects.filter(parent="").first()

    @classmethod
    def find(self, name):
        return self.objects.filter(name=name).first()

    @classmethod
    def find_by_id(self, idstring):
        return self.objects.filter(id=idstring).first()

    def __unicode__(self):
        return "{}/{}".format(self.name, self.path)

    def to_dict(self):
        return {
        }
