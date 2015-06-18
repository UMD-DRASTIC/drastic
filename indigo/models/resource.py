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
    url       = columns.Text(required=False)
    create_ts   = columns.DateTime()
    modified_ts = columns.DateTime()

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

        # Check the container exists
        collection = Collection.objects.filter(id=kwargs['container']).first()
        if not collection:
            raise NoSuchCollection("That collection does not exist")

        # Make sure parent/name are not in use.
        existing = self.objects.filter(container=kwargs['container']).all()
        if kwargs['name'] in [e['name'] for e in existing]:
            raise UniqueException("That name is in use in the current collection")

        return super(Resource, self).create(**kwargs)

    def update(self, **kwargs):
        kwargs['modified_ts'] = datetime.now()
        return super(Resource, self).update(**kwargs)

    @classmethod
    def find_by_id(self, idstring):
        return self.objects.filter(id=idstring).first()

    def __unicode__(self):
        return unicode(self.name)

    def to_dict(self):
        return  {
            "id": self.id,
            "name": self.name,
            "container_id": self.container,
            "checksum": self.checksum,
            "size": self.size,
            "metadata": [(k,v) for k,v in self.metadata.iteritems()],
            "create_ts": self.create_ts,
            "modified_ts": self.modified_ts
        }
