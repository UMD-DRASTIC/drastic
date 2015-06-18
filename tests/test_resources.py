import unittest

from indigo.models.collection import Collection
from indigo.models.user import User
from indigo.models.group import Group
from indigo.models.resource import Resource
from indigo.models.errors import UniqueException, NoSuchCollection

from nose.tools import raises

class ResourceTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def test_create_ok(self):
        coll = Collection.get_root_collection()

        resource = Resource.create(name='test_resource', container=coll.id)
        assert resource
        assert resource.name == 'test_resource'
        assert resource.container == coll.id


    @raises(NoSuchCollection)
    def test_create_fail(self):
        resource = Resource.create(name='invalid_resource', container="Wombles!")


    @raises(UniqueException)
    def test_create_dupe(self):
        coll = Collection.get_root_collection()
        resource = Resource.create(name='test_dupe', container=coll.id)
        assert resource
        resource = Resource.create(name='test_dupe', container=coll.id)

