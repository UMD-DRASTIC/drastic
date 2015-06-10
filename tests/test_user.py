import unittest

from indigo.models import User

class UserTest(unittest.TestCase):

    def test_create(self):
        User.create(username="test", password="password", email="test@localhost.local")
        user = User.find("test")
        assert user.username == "test"
        assert user.email == 'test@localhost.local'
        assert user.administrator == False
        assert user.active == True

