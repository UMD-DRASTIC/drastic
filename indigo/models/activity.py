"""
"""
import uuid
from datetime import datetime, timedelta

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from indigo.models.errors import UniqueException
from indigo.util import default_id

def default_time():
    return columns.TimeUUID.from_datetime(datetime.now())

def default_date():
    return datetime.now().strftime("%y%m%d")

def last_x_days(days=5):
    dt = datetime.now()
    dates = [dt + timedelta(days=-x) for x in xrange(1,5)] + [dt]
    return [d.strftime("%y%m%d") for d in dates]

class Activity(Model):
    id      = columns.Text(default=default_date, primary_key=True)
    html    = columns.Text()
    when    = columns.TimeUUID(primary_key=True,
                               default=default_time,
                               clustering_order="DESC")

    @classmethod
    def new(cls, content):
        return cls.create(html=content)

    @classmethod
    def recent(cls, count=20):
        return Activity.objects.filter(id__in=last_x_days())\
            .order_by("-when").all().limit(count)

    def __unicode__(self):
        return unicode(self.html)
