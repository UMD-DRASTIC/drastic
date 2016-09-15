"""Activity Model
"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


from datetime import (
    datetime,
    timedelta
)
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model


def default_time():
    """Generate a TimeUUID from the current local date and time"""
    return columns.TimeUUID.from_datetime(datetime.now())


def default_date():
    """Return a string representing current local the date"""
    return datetime.now().strftime("%y%m%d")


def last_x_days(days=5):
    """Return the last X days as string names YYMMDD in a list"""
    dt = datetime.now()
    dates = [dt + timedelta(days=-x) for x in xrange(1, days)] + [dt]
    return [d.strftime("%y%m%d") for d in dates]


class Activity(Model):
    """Activity Model"""
    id = columns.Text(default=default_date, primary_key=True)
    html = columns.Text()
    when = columns.TimeUUID(primary_key=True,
                            default=default_time,
                            clustering_order="DESC")

    @classmethod
    def new(cls, content):
        """"Create a new Activity"""
        return cls.create(html=content)

    @classmethod
    def recent(cls, count=20):
        """Return the last activities"""
        return Activity.objects.filter(id__in=last_x_days())\
            .order_by("-when").all().limit(count)

    def __unicode__(self):
        return unicode(self.html)
