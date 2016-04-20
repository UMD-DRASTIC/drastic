"""Notification Model

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


from datetime import (
    datetime,
    timedelta
)
import paho.mqtt.publish as publish
import logging
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from indigo.util import (
    datetime_serializer,
)

# Operations that could lead to a new notification
OP_CREATE = "create"
OP_DELETE = "delete"
OP_UPDATE = "update"
OP_INDEX = "index"
OP_MOVE = "move"

# Types of objects with the element needed to identify the object
OBJ_RESOURCE = "resource"         # path
OBJ_COLLECTION = "collection"     # path
OBJ_USER = "user"                 # id
OBJ_GROUP = "group"               # id


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


class Notification(Model):
    """Notification Model"""
    # The type of operation (Create, Delete, Update, Index, Move...)
    operation = columns.Text(primary_key=True)
    when = columns.TimeUUID(primary_key=True,
                            default=default_time,
                            clustering_order="DESC")
    # The type of the object concerned (Collection, Resource, User, Group, ...)
    object_type = columns.Text(primary_key=True)
    # The uuid of the object concerned, the key used to find the corresponding
    # object (path, uuid, ...)
    object_uuid = columns.Text(primary_key=True)
    # The user who initiates the operation
    user_uuid = columns.Text()
    # True if the corresponding worklow has been executed correctly (for Move
    # or indexing for instance)
    # True if nothing has to be done
    processed = columns.Boolean()
    # The payload of the message which is sent to MQTT
    payload = columns.Text()


    def __unicode__(self):
        return unicode(self.html)


    @classmethod
    def create_collection(cls, user_uuid, path, payload):
        """Create a new collection and publish the message on MQTT"""
        new = cls.new(operation=OP_CREATE,
                      object_type=OBJ_COLLECTION,
                      object_uuid=path,
                      user_uuid=user_uuid,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_CREATE, OBJ_COLLECTION, path, payload)
        return new


    @classmethod
    def create_group(cls, user_uuid, uuid, payload):
        """Create a new group and publish the message on MQTT"""
        new = cls.new(operation=OP_CREATE,
                      object_type=OBJ_GROUP,
                      object_uuid=uuid,
                      user_uuid=user_uuid,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_CREATE, OBJ_GROUP, uuid, payload)
        return new


    @classmethod
    def create_resource(cls, user_uuid, path, payload):
        """Create a new resource and publish the message on MQTT"""
        new = cls.new(operation=OP_CREATE,
                      object_type=OBJ_RESOURCE,
                      object_uuid=path,
                      user_uuid=user_uuid,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_CREATE, OBJ_RESOURCE, path, payload)
        return new


    @classmethod
    def create_user(cls, user_uuid, uuid, payload):
        """Create a new user and publish the message on MQTT"""
        new = cls.new(operation=OP_CREATE,
                      object_type=OBJ_USER,
                      object_uuid=uuid,
                      user_uuid=user_uuid,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_CREATE, OBJ_USER, uuid, payload)
        return new


    @classmethod
    def delete_collection(cls, user_uuid, path, payload):
        """Delete a collection and publish the message on MQTT"""
        new = cls.new(operation=OP_DELETE,
                      object_type=OBJ_COLLECTION,
                      object_uuid=path,
                      user_uuid=user_uuid,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_DELETE, OBJ_COLLECTION, path, payload)
        return new


    @classmethod
    def delete_group(cls, user_uuid, uuid, payload):
        """Delete a group and publish the message on MQTT"""
        new = cls.new(operation=OP_DELETE,
                      object_type=OBJ_GROUP,
                      object_uuid=uuid,
                      user_uuid=user_uuid,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_DELETE, OBJ_GROUP, uuid, payload)
        return new


    @classmethod
    def delete_resource(cls, user_uuid, path, payload):
        """Delete a resource and publish the message on MQTT"""
        new = cls.new(operation=OP_DELETE,
                      object_type=OBJ_RESOURCE,
                      object_uuid=path,
                      user_uuid=user_uuid,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_DELETE, OBJ_RESOURCE, path, payload)
        return new


    @classmethod
    def delete_user(cls, user_uuid, uuid, payload):
        """Delete a user and publish the message on MQTT"""
        new = cls.new(operation=OP_DELETE,
                      object_type=OBJ_USER,
                      object_uuid=uuid,
                      user_uuid=user_uuid,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_DELETE, OBJ_USER, uuid, payload)
        return new


    @classmethod
    def mqtt_publish(cls, notification, operation, object_type, object_uuid, payload):
        topic = u'{0}/{1}/{2}'.format(operation, object_type, object_uuid)
        # Clean up the topic by removing superfluous slashes.
        topic = '/'.join(filter(None, topic.split('/')))
        # Remove MQTT wildcards from the topic. Corner-case: If the collection name is made entirely of # and + and a
        # script is set to run on such a collection name. But that's what you get if you use stupid names for things.
        topic = topic.replace('#', '').replace('+', '')
        logging.info(u'Publishing on topic "{0}"'.format(topic))
        try:
            publish.single(topic, payload)
        except:
            notification.processed = False
            notification.save()
            logging.error(u'Problem while publishing on topic "{0}"'.format(topic))


    @classmethod
    def new(cls, **kwargs):
        """Create"""
        new = super(Notification, cls).create(**kwargs)
        return new


    @classmethod
    def recent(cls, count=20):
        """Return the last activities"""
        return Notification.objects.filter(id__in=last_x_days())\
            .order_by("-when").all().limit(count)


    @classmethod
    def update_collection(cls, user_uuid, path, payload):
        """Update a collection and publish the message on MQTT"""
        new = cls.new(operation=OP_UPDATE,
                      object_type=OBJ_COLLECTION,
                      object_uuid=path,
                      user_uuid=user_uuid,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_UPDATE, OBJ_COLLECTION, path, payload)
        return new


    @classmethod
    def update_group(cls, user_uuid, uuid, payload):
        """Update a group and publish the message on MQTT"""
        new = cls.new(operation=OP_UPDATE,
                      object_type=OBJ_GROUP,
                      object_uuid=uuid,
                      user_uuid=user_uuid,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_UPDATE, OBJ_GROUP, uuid, payload)
        return new


    @classmethod
    def update_resource(cls, user_uuid, path, payload):
        """Update a resource and publish the message on MQTT"""
        new = cls.new(operation=OP_UPDATE,
                      object_type=OBJ_RESOURCE,
                      object_uuid=path,
                      user_uuid=user_uuid,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_UPDATE, OBJ_RESOURCE, path, payload)
        return new


    @classmethod
    def update_user(cls, user_uuid, uuid, payload):
        """Update a user and publish the message on MQTT"""
        new = cls.new(operation=OP_UPDATE,
                      object_type=OBJ_USER,
                      object_uuid=uuid,
                      user_uuid=user_uuid,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_UPDATE, OBJ_USER, uuid, payload)
        return new
