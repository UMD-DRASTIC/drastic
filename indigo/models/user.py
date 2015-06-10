import uuid
from datetime import datetime

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from passlib.hash import pbkdf2_sha256

class User(Model):
    id       = columns.UUID(primary_key=True, default=uuid.uuid4)
    username = columns.Text(required=True, index=True)
    email    = columns.Text(required=True)
    password = columns.Text(required=True)
    administrator = columns.Boolean(required=True, default=False)
    active   = columns.Boolean(required=True, default=True)

    @classmethod
    def create(self, **kwargs):
        """
        We intercept the create call so that we can correctly
        hash the password into an unreadable form
        """
        kwargs['password'] = pbkdf2_sha256.encrypt(kwargs['password'],
                                                   rounds=200000,
                                                   salt_size=16)
        super(User, self).create(**kwargs)

    @classmethod
    def find(self, username):
        return self.objects.filter(username=username).first()

    def authenticate(self, password):
        return pbkdf2_sha256.verify(password, self.password) and self.active

    def __unicode__(self):
        return unicode(self.username)

    def to_dict(self):
        return {
            'username': self.username,
            'email': self.email,
            'administrator': self.administrator,
            'active': self.active,
        }
