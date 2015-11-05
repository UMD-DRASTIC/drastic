"""Group Model

Groups contain users and are used for determining what collections
and resources are available to those users.

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

# TODO: Improve the get users functions which will be slow if there are a lot
# of users


from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from indigo.models.errors import GroupConflictError
from indigo.util import default_uuid


class Group(Model):
    """Group Model"""
    id = columns.Text(primary_key=True, default=default_uuid)
    name = columns.Text(required=True, index=True)
    owner = columns.Text(required=True)

    @classmethod
    def create(cls, **kwargs):
        """Create a new group, raise an exception if the group already
        exists"""
        kwargs['name'] = kwargs['name'].strip()
        # Make sure name id not in use.
        existing = cls.objects.filter(name=kwargs['name']).first()
        if existing:
            raise GroupConflictError(kwargs['name'])
        grp = super(Group, cls).create(**kwargs)
        return grp

    @classmethod
    def find(cls, name):
        """Find a group by name"""
        return cls.objects.filter(name=name).first()

    @classmethod
    def find_by_id(cls, idstring):
        """Find a group by id"""
        return cls.objects.filter(id=idstring).first()

    @classmethod
    def find_by_ids(cls, idlist):
        """Find groups with a list of ids"""
        return cls.objects.filter(id__in=idlist).all()

    def __unicode__(self):
        return unicode(self.name)

    def get_users(self):
        """Get users of the group"""
        # Slow and ugly, not sure I like having to iterate
        # through all of the Users but the __in suffix for
        # queries won't let me query all users where this
        # objects ID appears in the User group field.
        from indigo.models import User
        return [u for u in User.objects.all()
                if u.active and self.id in u.groups]

    def get_usernames(self):
        """Get a list of usernames of the group"""
        # Slow and ugly, not sure I like having to iterate
        # through all of the Users but the __in suffix for
        # queries won't let me query all users where this
        # objects ID appears in the User group field.
        from indigo.models import User
        return [u.username for u in User.objects.all()
                if u.active and self.id in u.groups]

    def to_dict(self):
        """Return a dictionary that represents the group"""
        return {
            'id': self.id,
            'name': self.name,
            'members': self.get_usernames()
        }
