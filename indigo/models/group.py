"""
Groups contain users and are used for determining what collections
and resources are available to those users.
"""
import uuid
from datetime import datetime

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from indigo.models.errors import UniqueException
from indigo.util import default_id


class Group(Model):
    id       = columns.Text(primary_key=True, default=default_id)
    name = columns.Text(required=True, index=True)
    owner    = columns.Text(required=True)

    @classmethod
    def create(self, **kwargs):
        kwargs['name'] = kwargs['name'].strip()

        # Make sure name id not in use.
        existing = self.objects.filter(name=kwargs['name']).first()
        if existing:
            raise UniqueException("That name is in use in the current collection")



        return super(Group, self).create(**kwargs)

    @classmethod
    def find(self, name):
        return self.objects.filter(name=name).first()

    @classmethod
    def find_by_id(self, idstring):
        return self.objects.filter(id=idstring).first()

    @classmethod
    def find_by_ids(self, idlist):
        return self.objects.filter(id__in=idlist).all()

    def get_users(self):
        # Slow and ugly, not sure I like having to iterate
        # through all of the Users but the __in suffix for
        # queries won't let me query all users where this
        # objects ID appears in the User group field.
        from indigo.models import User
        return [u for u in User.objects.all() if u.active and self.id in u.groups]

    def __unicode__(self):
        return unicode(self.name)

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
        }
