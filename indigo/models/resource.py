import uuid
from datetime import datetime

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from indigo.models.errors import UniqueException, NoSuchCollection
from indigo.util import default_id

class Resource(Model):
    id        = columns.Text(primary_key=True, default=default_id)
    name      = columns.Text(required=True, index=True)
    container = columns.Text(required=True, index=True)
    checksum  = columns.Text(required=False)
    size      = columns.Integer(required=False, default=0, index=True)
    metadata  = columns.Map(columns.Text, columns.Text, index=True)
    mimetype  = columns.Text(required=False)
    url       = columns.Text(required=False)
    create_ts   = columns.DateTime()
    modified_ts = columns.DateTime()

    # The access columns contain lists of group IDs that are allowed
    # the specified permission. If the lists have at least one entry
    # then access is restricted, if there are no entries in a particular
    # list, then access is granted to all (authenticated users)
    read_access   = columns.List(columns.Text)
    edit_access   = columns.List(columns.Text)
    write_access  = columns.List(columns.Text)
    delete_access = columns.List(columns.Text)


    @classmethod
    def create(self, **kwargs):
        """
        When we create a resource, the minimum we require is a name
        and a container. There is little chance of getting trustworthy
        versions of any of the other data at creation stage.
        """
        from indigo.models.collection import Collection

        # TODO: Handle unicode chars in the name
        kwargs['name'] = kwargs['name'].strip()
        kwargs['create_ts'] = datetime.now()
        kwargs['modified_ts'] = kwargs['create_ts']
        # Check the container exists
        collection = Collection.objects.filter(id=kwargs['container']).first()
        if not collection:
            raise NoSuchCollection("That collection does not exist")

        # Make sure parent/name are not in use.
        existing = self.objects.filter(container=kwargs['container']).all()
        if kwargs['name'] in [e['name'] for e in existing]:
            raise UniqueException("That name is in use in the current collection")

        return super(Resource, self).create(**kwargs)

    def user_can(self, user, action):
        """
        User can perform the action if any of the user's group IDs
        appear in this list for 'action'_access in this object.
        """
        if user.administrator:
            return True

        l = getattr(self, '{}_access'.format(action))
        if len(l) and not len(user.groups):
            # Group access required, user not in any groups
            return False
        if not len(l):
            # Group access not required
            return True

        # if groups has less than user.groups then it has had a group
        # removed, it confirms presence in l
        groups = set(user.groups) - set(l)
        return len(groups) < len(user.groups)


    def update(self, **kwargs):
        kwargs['modified_ts'] = datetime.now()
        return super(Resource, self).update(**kwargs)

    @classmethod
    def find_by_id(self, idstring):
        return self.objects.filter(id=idstring).first()

    def __unicode__(self):
        return unicode(self.name)

    def to_dict(self, user=None):
        data =   {
            "id": self.id,
            "name": self.name,
            "container_id": self.container,
            "checksum": self.checksum,
            "size": self.size,
            "metadata": [(k,v) for k,v in self.metadata.iteritems()],
            "create_ts": self.create_ts,
            "modified_ts": self.modified_ts,
            "mimetype": self.mimetype or "application/octet-stream"
        }
        if user:
            data['can_read'] = self.user_can(user, "read")
            data['can_write'] = self.user_can(user, "write")
            data['can_edit'] = self.user_can(user, "edit")
            data['can_delete'] = self.user_can(user, "delete")
        return data

