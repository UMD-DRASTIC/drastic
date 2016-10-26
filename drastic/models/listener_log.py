"""Listener Logging Model

"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


import json
import logging
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import (
    connection,
    Model
    )
from cassandra.util import uuid_from_time
from cassandra.util import datetime_from_uuid1
from cassandra.query import SimpleStatement

from drastic import get_config
from drastic.util import (
    datetime_serializer,
    default_time,
    default_date,
    last_x_days
)



class ListenerLog(Model):
    """Listener Log Model"""
    script_name = columns.Text(partition_key=True)
    when = columns.TimeUUID(primary_key=True,
                            default=default_time,
                            clustering_order="DESC")
    stdout = columns.Text()
    stderr = columns.Text()


    @classmethod
    def recent(cls, script_name, count=20):
        """Return the last logs"""
        cfg = get_config(None)
        session = connection.get_session()
        keyspace = cfg.get('KEYSPACE', 'drastic')
        session.set_keyspace(keyspace)
        # I couldn't find how to disable paging in cqlengine in the "model" view
        # so I create the cal query directly
        query = SimpleStatement(u"""SELECT * from listener_log WHERE
            script_name = '{}'
            ORDER BY when DESC
            limit {}""".format(script_name,
                               count)
            )
        # Disable paging for this query (we use IN and ORDER BY in the same
        # query
        query.fetch_size = None
        res = []
        for row in session.execute(query):
            res.append(ListenerLog(**row).to_dict())
        return res

    def to_dict(self):
        """Return a dictionary which describes a log for the web ui"""        
        data = {
            'when': datetime_from_uuid1(self.when),
            'script_name': self.script_name,
            'stdout': self.stdout,
            'stderr': self.stderr,
        }
        return data


