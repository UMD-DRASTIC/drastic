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
import json

from indigo.models.errors import GroupConflictError
from indigo.util import (
    datetime_serializer,
    default_uuid
)


class Group(Model):
    """Group Model"""
    uuid = columns.Text(default=default_uuid)
    name = columns.Text(primary_key=True, required=True)
#     owner = columns.Text(required=True)

    def add_user(self, username):
        return self.add_users([username])

    def add_users(self, ls_users):
        """Add a list of users to a group
        Return 3 lists:
          - added for the username which were added
          - already_there for username already in the group
          - not_added for username not found"""
        from indigo.models import User
        added = []
        not_added = []
        already_there = []
        for username in ls_users:
            user = User.find(username)
            if user:
                if self.name not in user.get_groups():
                    user.add_group(self.name)
                    added.append(username)
                else:
                    already_there.append(username)
            else:
                not_added.append(username)
        return added, not_added, already_there

    @classmethod
    def create(cls, **kwargs):
        """Create a new group, raise an exception if the group already
        exists"""
        from indigo.models import Notification
        kwargs['name'] = kwargs['name'].strip()
        if 'username' in kwargs:
            username = kwargs['username']
            del kwargs['username']
        else:
            username = None
        # Make sure name id not in use.
        existing = cls.objects.filter(name=kwargs['name']).first()
        if existing:
            raise GroupConflictError(kwargs['name'])
        grp = super(Group, cls).create(**kwargs)
        state = grp.mqtt_get_state()
        payload = grp.mqtt_payload({}, state)
        Notification.create_group(username, grp.name, payload)
        return grp

    @classmethod
    def find(cls, name):
        """Find a group by name"""
        return cls.objects.filter(name=name).first()

    @classmethod
    def find_all(cls, namelist):
        """Find groups with a list of names"""
        return cls.objects.filter(name__in=namelist).all()

    def __unicode__(self):
        return unicode(self.name)

    def delete(self, username=None):
        # Slow and ugly,
        from indigo.models import Notification
        from indigo.models import User
        state = self.mqtt_get_state()
        for u in User.objects.all():
            if self.name in u.groups:
                u.groups.remove(self.name)
                u.save()
        super(Group, self).delete()
        payload = self.mqtt_payload(state, {})
        Notification.delete_group(username, self.name, payload)

    def get_users(self):
        """Get users of the group"""
        # Slow and ugly, not sure I like having to iterate
        # through all of the Users but the __in suffix for
        # queries won't let me query all users where this
        # objects ID appears in the User group field.
        from indigo.models import User
        return [u for u in User.objects.all()
                if u.active and self.name in u.groups]

    def get_usernames(self):
        """Get a list of usernames of the group"""
        # Slow and ugly, not sure I like having to iterate
        # through all of the Users but the __in suffix for
        # queries won't let me query all users where this
        # objects ID appears in the User group field.
        from indigo.models import User
        return [u.name for u in User.objects.all()
                if u.active and self.name in u.groups]

    def mqtt_get_state(self):
        """Get the user state for the payload"""
        payload = dict()
        payload['uuid'] = self.uuid
        payload['name'] = self.name
        payload['members'] =  self.get_usernames()
        return payload


    def mqtt_payload(self, pre_state, post_state):
        """Get a string version of the payload of the message"""
        payload = dict()
        payload['pre'] = pre_state
        payload['post'] = post_state
        return json.dumps(payload, default=datetime_serializer)

    def rm_user(self, username):
        return self.rm_users([username])

    def rm_users(self, ls_users):
        """Remove a list of users from the group
        Return 3 lists:
            removed for the username who were removed
            not_there for the username who weren't in the gr
            not_exist for the usernames who doesn't exist"""
        from indigo.models import User
        not_exist = []
        removed = []
        not_there = []
        for username in ls_users:
            user = User.find(username)
            if user:
                if self.name in user.get_groups():
                    user.rm_group(self.name)
                    removed.append(username)
                else:
                    not_there.append(username)
            else:
                not_exist.append(username)
        return removed, not_there, not_exist

    def to_dict(self):
        """Return a dictionary that represents the group"""
        return {
            'uuid': self.uuid,
            'name': self.name,
            'members': self.get_usernames()
        }

    def update(self, **kwargs):
        """Update a group"""
        from indigo.models import Notification
        pre_state = self.mqtt_get_state()
        if 'username' in kwargs:
            username = kwargs['username']
            del kwargs['username']
        else:
            username = None
        super(Group, self).update(**kwargs)
        group = Group.find(self.name)
        post_state = group.mqtt_get_state()
        payload = group.mqtt_payload(pre_state, post_state)
        Notification.update_group(username, group.name, payload)
        return self
