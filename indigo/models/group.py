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
    uuid = columns.Text(primary_key=True, default=default_uuid)
    name = columns.Text(required=True, index=True)
#     owner = columns.Text(required=True)

    @classmethod
    def create(cls, **kwargs):
        """Create a new group, raise an exception if the group already
        exists"""
        from indigo.models import Notification
        kwargs['name'] = kwargs['name'].strip()
        if 'user_uuid' in kwargs:
            user_uuid = kwargs['user_uuid']
            del kwargs['user_uuid']
        else:
            user_uuid = None
        # Make sure name id not in use.
        existing = cls.objects.filter(name=kwargs['name']).first()
        if existing:
            raise GroupConflictError(kwargs['name'])
        grp = super(Group, cls).create(**kwargs)
        state = grp.mqtt_get_state()
        payload = grp.mqtt_payload({}, state)
        # user_uuid is the id of the user who did the operation
        # user.uuid is the id of the new user
        Notification.create_group(user_uuid, grp.uuid, payload)
        return grp

    @classmethod
    def find(cls, name):
        """Find a group by name"""
        return cls.objects.filter(name=name).first()

    @classmethod
    def find_by_uuid(cls, idstring):
        """Find a group by id"""
        return cls.objects.filter(uuid=idstring).first()

    @classmethod
    def find_by_ids(cls, idlist):
        """Find groups with a list of ids"""
        return cls.objects.filter(uuid__in=idlist).all()

    def __unicode__(self):
        return unicode(self.name)

    def delete(self, user_uuid=None):
        # Slow and ugly,
        from indigo.models import Notification
        from indigo.models import User
        state = self.mqtt_get_state()
        for u in User.objects.all():
            if self.uuid in u.groups:
                u.groups.remove(self.uuid)
                u.save()
        super(Group, self).delete()
        payload = self.mqtt_payload(state, {})
        Notification.delete_group(user_uuid, self.uuid, payload)

    def get_users(self):
        """Get users of the group"""
        # Slow and ugly, not sure I like having to iterate
        # through all of the Users but the __in suffix for
        # queries won't let me query all users where this
        # objects ID appears in the User group field.
        from indigo.models import User
        return [u for u in User.objects.all()
                if u.active and self.uuid in u.groups]

    def get_usernames(self):
        """Get a list of usernames of the group"""
        # Slow and ugly, not sure I like having to iterate
        # through all of the Users but the __in suffix for
        # queries won't let me query all users where this
        # objects ID appears in the User group field.
        from indigo.models import User
        return [u.name for u in User.objects.all()
                if u.active and self.uuid in u.groups]

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
        if 'user_uuid' in kwargs:
            user_uuid = kwargs['user_uuid']
            del kwargs['user_uuid']
        else:
            user_uuid = None
        super(Group, self).update(**kwargs)
        group = Group.find_by_uuid(self.uuid)
        post_state = group.mqtt_get_state()
        payload = group.mqtt_payload(pre_state, post_state)
        Notification.update_group(user_uuid, group.uuid, payload)
        return self
