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

        results = SearchIndex.find(["test", "root"])
        assert len(results) == 1
        assert results[0]["id"] == coll.id
        assert results[0]["hit_count"] == 2

        SearchIndex.reset(coll.id)

        results = SearchIndex.find(["test", "root"])
        assert len(results) == 0
