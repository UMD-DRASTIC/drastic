"""Group Model

Groups contain users and are used for determining what collections
and resources are available to those users.
"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from drastic.models.errors import GroupConflictError
from drastic.util import default_uuid


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
        return super(Group, cls).create(**kwargs)

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
        from drastic.models import User
        return [u for u in User.objects.all()
                if u.active and self.id in u.groups]

    def to_dict(self):
        """Return a dictionary that represents the group"""
        return {
            'id': self.id,
            'name': self.name,
        }
