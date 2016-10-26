"""Notification Model

"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


class Notification(Model):
    """Notification Model"""
    date = columns.Text(default=default_date, partition_key=True)
    when = columns.TimeUUID(primary_key=True,
                            default=default_time,
                            clustering_order="DESC")
    # The type of operation (Create, Delete, Update, Index, Move...)
    operation = columns.Text(primary_key=True)
    # The type of the object concerned (Collection, Resource, User, Group, ...)
    object_type = columns.Text(primary_key=True)
    # The uuid of the object concerned, the key used to find the corresponding
    # object (path, uuid, ...)
    object_uuid = columns.Text(primary_key=True)
    
    # The user who initiates the operation
    username = columns.Text()
    # True if the corresponding worklow has been executed correctly (for Move
    # or indexing for instance)
    # True if nothing has to be done
    processed = columns.Boolean()
    # The payload of the message which is sent to MQTT
    payload = columns.Text()


    def __unicode__(self):
        return unicode(self.html)


    @classmethod
    def create_collection(cls, username, path, payload):
        """Create a new collection and publish the message on MQTT"""
        new = cls.new(operation=OP_CREATE,
                      object_type=OBJ_COLLECTION,
                      object_uuid=path,
                      username=username,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_CREATE, OBJ_COLLECTION, path, payload)
        return new


    @classmethod
    def create_group(cls, username, uuid, payload):
        """Create a new group and publish the message on MQTT"""
        new = cls.new(operation=OP_CREATE,
                      object_type=OBJ_GROUP,
                      object_uuid=uuid,
                      username=username,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_CREATE, OBJ_GROUP, uuid, payload)
        return new


    @classmethod
    def create_resource(cls, username, path, payload):
        """Create a new resource and publish the message on MQTT"""
        new = cls.new(operation=OP_CREATE,
                      object_type=OBJ_RESOURCE,
                      object_uuid=path,
                      username=username,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_CREATE, OBJ_RESOURCE, path, payload)
        return new


    @classmethod
    def create_user(cls, username, uuid, payload):
        """Create a new user and publish the message on MQTT"""
        new = cls.new(operation=OP_CREATE,
                      object_type=OBJ_USER,
                      object_uuid=uuid,
                      username=username,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_CREATE, OBJ_USER, uuid, payload)
        return new


    @classmethod
    def delete_collection(cls, username, path, payload):
        """Delete a collection and publish the message on MQTT"""
        new = cls.new(operation=OP_DELETE,
                      object_type=OBJ_COLLECTION,
                      object_uuid=path,
                      username=username,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_DELETE, OBJ_COLLECTION, path, payload)
        return new


    @classmethod
    def delete_group(cls, username, uuid, payload):
        """Delete a group and publish the message on MQTT"""
        new = cls.new(operation=OP_DELETE,
                      object_type=OBJ_GROUP,
                      object_uuid=uuid,
                      username=username,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_DELETE, OBJ_GROUP, uuid, payload)
        return new


    @classmethod
    def delete_resource(cls, username, path, payload):
        """Delete a resource and publish the message on MQTT"""
        new = cls.new(operation=OP_DELETE,
                      object_type=OBJ_RESOURCE,
                      object_uuid=path,
                      username=username,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_DELETE, OBJ_RESOURCE, path, payload)
        return new


    @classmethod
    def delete_user(cls, username, uuid, payload):
        """Delete a user and publish the message on MQTT"""
        new = cls.new(operation=OP_DELETE,
                      object_type=OBJ_USER,
                      object_uuid=uuid,
                      username=username,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_DELETE, OBJ_USER, uuid, payload)
        return new


    def tmpl(self):
        return TEMPLATES[self.operation][self.object_type]


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
            notification.update(processed=False)
            logging.error(u'Problem while publishing on topic "{0}"'.format(topic))


    @classmethod
    def new(cls, **kwargs):
        """Create"""
        new = super(Notification, cls).create(**kwargs)
        return new


    @classmethod
    def recent(cls, count=20):
        """Return the last activities"""
#         return Notification.objects.filter(date__in=last_x_days())\
#             .order_by("-when").all().limit(count)
        cfg = get_config(None)
        session = connection.get_session()
        keyspace = cfg.get('KEYSPACE', 'drastic')
        session.set_keyspace(keyspace)
        # I couldn't find how to disable paging in cqlengine in the "model" view
        # so I create the cal query directly
        query = SimpleStatement(u"""SELECT * from Notification WHERE
            date IN ({})
            ORDER BY when DESC
            limit {}""".format(
                ",".join(["'%s'" % el for el in last_x_days()]),
                count)
            )
        # Disable paging for this query (we use IN and ORDER BY in the same
        # query
        query.fetch_size = None
        res = []
        for row in session.execute(query):
            res.append(Notification(**row).to_dict())
        return res

    def to_dict(self, user=None):
        """Return a dictionary which describes a notification for the web ui"""
        data = {
            'date': self.date,
            'when': self.when,
            'operation': self.operation,
            'object_type': self.object_type,
            'object_uuid': self.object_uuid,
            'username': self.username,
            'tmpl': self.tmpl(),
            'payload': json.loads(self.payload)
        }
        return data


    @classmethod
    def update_collection(cls, username, path, payload):
        """Update a collection and publish the message on MQTT"""
        new = cls.new(operation=OP_UPDATE,
                      object_type=OBJ_COLLECTION,
                      object_uuid=path,
                      username=username,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_UPDATE, OBJ_COLLECTION, path, payload)
        return new


    @classmethod
    def update_group(cls, username, uuid, payload):
        """Update a group and publish the message on MQTT"""
        new = cls.new(operation=OP_UPDATE,
                      object_type=OBJ_GROUP,
                      object_uuid=uuid,
                      username=username,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_UPDATE, OBJ_GROUP, uuid, payload)
        return new


    @classmethod
    def update_resource(cls, username, path, payload):
        """Update a resource and publish the message on MQTT"""
        new = cls.new(operation=OP_UPDATE,
                      object_type=OBJ_RESOURCE,
                      object_uuid=path,
                      username=username,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_UPDATE, OBJ_RESOURCE, path, payload)
        return new


    @classmethod
    def update_user(cls, username, uuid, payload):
        """Update a user and publish the message on MQTT"""
        new = cls.new(operation=OP_UPDATE,
                      object_type=OBJ_USER,
                      object_uuid=uuid,
                      username=username,
                      processed=True,
                      payload=payload)
        cls.mqtt_publish(new, OP_UPDATE, OBJ_USER, uuid, payload)
        return new
