import uuid
from datetime import datetime

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from passlib.hash import pbkdf2_sha256

from indigo.models.errors import UniqueException
from indigo.util import default_id


class User(Model):
    id       = columns.Text(primary_key=True, default=default_id)
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
        if self.objects.filter(username=kwargs['username']).count():
            raise UniqueException("Username '{}' already in use".format(kwargs['username']))

        return super(User, self).create(**kwargs)

    def save(self, **kwargs):
        if "update_fields" in kwargs:
            del kwargs["update_fields"]
        super(User, self).save(**kwargs)

    def is_authenticated(self):
        return True

    def is_active(self):
        return self.active

    def get_full_name(self):
        return self.username

    @classmethod
    def find(self, username):
        return self.objects.filter(username=username).first()

    @classmethod
    def find_by_id(self, idstring):
        return self.objects.filter(id=idstring).first()

    def authenticate(self, password):
        return pbkdf2_sha256.verify(password, self.password) and self.active

    def __unicode__(self):
        return unicode(self.username)

    def to_dict(self):
        return {
            'id': self.id,
            'username': self.username,
            'email': self.email,
            'administrator': self.administrator,
            'active': self.active,
        }
