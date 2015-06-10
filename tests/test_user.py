import unittest

from indigo.models import User
from indigo.models.errors import UniqueException

from nose.tools import raises

class UserTest(unittest.TestCase):

    def test_create(self):
        User.create(username="test", password="password", email="test@localhost.local")
        user = User.find("test")
        assert user.username == "test"
        assert user.email == 'test@localhost.local'
        assert user.administrator == False
        assert user.active == True

    @raises(UniqueException)
    def test_create_fail(self):
        User.create(username="test", password="password", email="test@localhost.local")
        user = User.find("test")
        User.create(username="test", password="password", email="test@localhost.local")
        user = User.find("test")


    def test_authenticate(self):
        User.create(username="test_auth", password="password", email="test@localhost.local")
        user = User.find("test_auth")
        assert user.authenticate("password")

    def test_authenticate_fail(self):
        User.create(username="test_auth_fail", password="password", email="test@localhost.local")
        user = User.find("test_auth")
        assert not user.authenticate("not the password")

