import unittest

from indigo.models.user import User
from indigo.models.group import Group
from indigo.models.errors import UserConflictError

from nose.tools import raises

class UserTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def test_create(self):
        user = User.create(username="test", password="password", email="test@localhost.local", quick=True)

        assert user.name == "test"
        assert user.email == 'test@localhost.local'
        assert user.administrator == False
        assert user.active == True

    @raises(UserConflictError)
    def test_create_fail(self):
        User.create(username="test", password="password", email="test@localhost.local", quick=True)
        User.create(username="test", password="password", email="test@localhost.local", quick=True)

    def test_authenticate(self):
        user = User.create(username="test_auth", password="password", email="test@localhost.local", quick=True)
        assert user.authenticate("password")

    def test_authenticate_fail(self):
        user = User.create(username="test_auth_fail", password="password", email="test@localhost.local", quick=True)
        assert not user.authenticate("not the password")

    def test_group_membership(self):
        user = User.create(username="test_group", password="password", email="test@localhost.local", groups=[], quick=True)
        assert user
        group = Group.create(name="test_group_1")
        user.update(groups=[group.id])

        # Refetch the user
        user = User.find("test_group")
        assert group.id in user.groups

        groups = Group.find_by_ids(user.groups)
        assert [g.id for g in groups] == user.groups

        users = group.get_users()
        assert users[0].id == user.id
