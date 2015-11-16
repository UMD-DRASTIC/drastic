"""User Model

Copyright 2015 Archive Analytics Solutions

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""


from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from passlib.hash import pbkdf2_sha256

from indigo.models.group import Group
from indigo.models.errors import UserConflictError
from indigo.util import default_uuid


class User(Model):
    """User Model"""
    id = columns.Text(primary_key=True, default=default_uuid)
#     username = columns.Text(required=True)
    name = columns.Text(required=True, index=True)
    email = columns.Text(required=True)
    password = columns.Text(required=True)
    administrator = columns.Boolean(required=True, default=False)
    active = columns.Boolean(required=True, default=True)
    groups = columns.List(columns.Text, index=True)

    @classmethod
    def create(cls, **kwargs):
        """Create a user

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
        if cls.objects.filter(name=kwargs['name']).count():
            raise UserConflictError(kwargs['name'])

        # The following does not return a new instance of User, and I have
        # singularly failed to find out why, as it works elsewhere.
        # return super(User, cls).create(**kwargs)
        user = User(**kwargs)
        user.save()
        return user

    @classmethod
    def find(cls, name):
        """Find a user from his name"""
        return cls.objects.filter(name=name).first()

    @classmethod
    def find_by_id(cls, idstring):
        """Find a user from his id"""
        return cls.objects.filter(id=idstring).first()

    def __unicode__(self):
        return unicode(self.name)

    def authenticate(self, password):
        """Verify if the user is authenticated"""
        return pbkdf2_sha256.verify(password, self.password) and self.active

    def get_full_name(self):
        """Return user full name"""
        return self.name

    def is_active(self):
        """Check if the user is active"""
        return self.active

    def is_authenticated(self):
        """Check if the user is authenticated"""
        return True

    def save(self, **kwargs):
        """Save modifications in Cassandra"""
        if "update_fields" in kwargs:
            del kwargs["update_fields"]
        super(User, self).save(**kwargs)

    def to_dict(self):
        """Return a dictionary which describes a resource for the web ui"""
        return {
            'id': self.id,
            'name': self.name,
            'email': self.email,
            'administrator': self.administrator,
            'active': self.active,
            'groups': [g.to_dict() for g in Group.find_by_ids(self.groups)]
        }

    def update(self, **kwargs):
        """Update a user"""
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
