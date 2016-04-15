import unittest

from indigo.models.collection import Collection
from indigo.models.user import User
from indigo.models.group import Group

from nose.tools import raises

class NodeTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def test_create_root(self):
        coll = Collection.create(name="test_root", parent=None, path="/")
        assert coll.name == "test_root"
        assert coll.path == '/'
        assert coll.parent is None

        # Make sure this is the root collection
        root = Collection.get_root_collection()
        assert root.id == coll.id

    def test_create_with_children(self):
        coll = Collection.find("test_root")
        assert coll.name == "test_root"
        assert coll.is_root

        child1 = Collection.create(name="child1", parent=str(coll.id))
        child2 = Collection.create(name="child2", parent=str(coll.id))

        assert child1.get_parent_collection().id == coll.id
        assert child2.get_parent_collection().id == coll.id

        assert child1.path == '/child1/'
        assert child2.path == '/child2/'

        children = coll.get_child_collections()
        assert len(children) == 2

        assert coll.get_child_collection_count() == 2

    def test_perms_for_collection(self):
        User.create(username="test_coll", password="password", email="test@localhost.local", groups=[], quick=True)
        user = User.find("test_coll")
        group = Group.create(name="test_group_coll")
        user.update(groups=[group.id])

        root = Collection.find("test_root")
        coll = Collection.create(name="perm_check", parent=str(root.id), read_access=[group.id])

        # User can read collection coll if user is in a group also in coll's read_access
        assert coll.user_can(user, "read") == True

    def test_perms_for_collection_fail_user_no_group(self):
        User.create(username="test_coll2", password="password", email="test@localhost.local", groups=[], quick=True)
        user = User.find("test_coll2")

        root = Collection.find("test_root")
        group = Group.create(name="test_group_coll2")
        coll = Collection.create(name="perm_check2", parent=str(root.id), read_access=[group.id])

        # User can read collection coll if user is in a group also in coll's read_access
        assert coll.user_can(user, "read") == False

    def test_perms_for_collection_success_collection_no_group(self):
        User.create(username="test_coll3", password="password", email="test@localhost.local", groups=[], quick=True)
        user = User.find("test_coll3")

        root = Collection.find("test_root")
        coll = Collection.create(name="perm_check3", parent=str(root.id), read_access=[])

        # User can read collection coll if user is in a group also in coll's read_access
        assert coll.user_can(user, "read") == True
