import unittest

from indigo.models.search import SearchIndex
from indigo.models.collection import Collection
from indigo.models.user import User
from indigo.models.group import Group


from nose.tools import raises

class SearchTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def test_index(self):
        coll = Collection.create(name="test_root", parent=None, path="/")

        res_count = SearchIndex.index(coll, ['name'])
        assert res_count == 2, res_count

        User.create(username="test_index_user", password="password", email="test@localhost.local", quick=True)
        user = User.find("test_index_user")

        results = SearchIndex.find(["test", "root"], user)
        assert len(results) == 1
        assert results[0]["id"] == coll.id
        assert results[0]["hit_count"] == 2

        SearchIndex.reset(coll.id)

        results = SearchIndex.find(["test", "root"], user)
        assert len(results) == 0

    def test_permissions(self):
        coll = Collection.find("test_root")

        User.create(username="protected_test_owner", password="password", email="test@localhost.local", quick=True)
        User.create(username="protected_reader", password="password", email="test@localhost.local", quick=True)
        owning_user = User.find("protected_test_owner")
        reading_user = User.find("protected_reader")
        assert owning_user
        assert reading_user

        group = Group.create(name="protected_group")
        Collection.create(name="protected", parent=coll.id, read_access=[group.id] )

        c = Collection.find("protected")
        res_count = SearchIndex.index(c, ['name', 'metadata'])
        assert res_count == 1

        results = SearchIndex.find(["protected"], reading_user)
        assert len(results) == 0, results
