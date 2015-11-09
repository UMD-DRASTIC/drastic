import unittest

from indigo.models.collection import Collection
from indigo.models.user import User
from indigo.models.group import Group
from indigo.models.resource import Resource
from indigo.models.errors import (
    ResourceConflictError,
    NoSuchCollection
)

from nose.tools import raises

class ResourceTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def test_create_ok(self):
        coll = Collection.get_root_collection()

        resource = Resource.create(name='test_resource', container=coll.id)
        assert resource
        assert resource.name == 'test_resource'
        assert resource.container == coll.id


    @raises(ResourceConflictError)
    def test_create_fail(self):
        resource = Resource.create(name='invalid_resource', container="Wombles!")


    @raises(ResourceConflictError)
    def test_create_dupe(self):
        coll = Collection.get_root_collection()
        resource = Resource.create(name='test_dupe', container=coll.id)
        assert resource
        resource = Resource.create(name='test_dupe', container=coll.id)


    def test_permission_ok(self):
        coll = Collection.get_root_collection()
        user = User.create(username="test_res_user", password="password", email="test@localhost.local", groups=[], quick=True)
        group = Group.create(name="test_group_resourdce")
        user.update(groups=[group.id])

        resource = Resource.create(name='new_test_resource', container=coll.id, read_access=[group.id])
        assert resource.user_can(user, "read")

    def test_permission_public_ok(self):
        coll = Collection.get_root_collection()
        user = User.create(username="test_res_user_public", password="password", email="test@localhost.local", groups=[], quick=True)

        resource = Resource.create(name='new_test_resource_public', container=coll.id)
        assert resource.user_can(user, "read")