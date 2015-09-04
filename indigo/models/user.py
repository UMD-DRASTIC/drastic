import uuid
from datetime import datetime

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from passlib.hash import pbkdf2_sha256

from indigo.models.group import Group
from indigo.models.errors import UniqueException
from indigo.util import default_uuid


class User(Model):
    id       = columns.Text(primary_key=True, default=default_uuid)
    username = columns.Text(required=True, index=True)
    email    = columns.Text(required=True)
    password = columns.Text(required=True)
    administrator = columns.Boolean(required=True, default=False)
    active   = columns.Boolean(required=True, default=True)
    groups   = columns.List(columns.Text, index=True)

    @classmethod
    def create(cls, **kwargs):
        """
        We intercept the create call so that we can correctly
        hash the password into an unreadable form
        """
        if 'quick' in kwargs:
            rounds = 1
            size = 1
            kwargs.pop('quick')
        else:
            rounds = 200000
            size = 16
        kwargs['password'] = pbkdf2_sha256.encrypt(kwargs['password'],
                                                   rounds=rounds,
                                                   salt_size=size)
        if cls.objects.filter(username=kwargs['username']).count():
            raise UniqueException("Username '{}' already in use".format(kwargs['username']))

        # The following does not return a new instance of User, and I have
        # singularly failed to find out why, as it works elsewhere.
        #return super(User, cls).create(**kwargs)
        user = User(**kwargs)
        user.save()
        return user

    def update(self, **kwargs):
        # If we want to update the password we need to encrypt it first
        if "password" in kwargs:
            if 'quick' in kwargs:
                rounds = 1
                size = 1
                kwargs.pop('quick')
            else:
                rounds = 200000
                size = 16
            kwargs['password'] = pbkdf2_sha256.encrypt(kwargs['password'],
                                                       rounds=rounds,
                                                       salt_size=size)
        super(User, self).update(**kwargs)

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
            'groups': [g.to_dict for g in Group.find_by_ids(self.groups)]
        }
