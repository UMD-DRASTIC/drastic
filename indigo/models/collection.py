import uuid
from datetime import datetime

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from indigo.models import Resource
from indigo.util import default_id
from indigo.models.errors import UniqueException

class Collection(Model):
    id       = columns.Text(primary_key=True, default=default_id)
    name     = columns.Text(required=True, index=True)

    # ID of the parent collection, if "" then is the root element
    parent   = columns.Text(required=False, index=True)
    path     = columns.Text(required=True)
    is_root  = columns.Boolean(default=False, index=True)

    @classmethod
    def create(self, **kwargs):
        """
        We intercept the create call so that we can correctly
        set the is_root (and path) for the collection
        """
        if not kwargs.get('parent'):
            kwargs['is_root'] = True
            kwargs['path'] = '/'
        else:
            parent = Collection.find_by_id(kwargs['parent'])
            kwargs['path'] = "{}{}/".format(parent.path, kwargs['name'])

        return super(Collection, self).create(**kwargs)

    def get_child_collections(self):
        return Collection.objects.filter(parent=self.id).all()

    def get_child_collection_count(self):
        return Collection.objects.filter(parent=self.id).count()

    def get_child_resources(self):
        return Resource.objects.filter(container=self.id).all()

    def get_child_resource_count(self):
        return Resource.objects.filter(container=self.id).count()

    def get_parent_collection(self):
        return Collection.find_by_id(self.parent)

    @classmethod
    def get_root_collection(self):
        return Collection.objects.filter(is_root=True).first()

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
