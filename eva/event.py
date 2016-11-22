import uuid
import datetime
import dateutil.parser


class Event(object):
    """!
    @brief Base class for events, based on messages from the Kafka message queue.
    """
    def __init__(self, message, data, **kwargs):
        self._id = uuid.uuid4()
        self.message = message
        self.data = data
        self.kwargs = kwargs

    def __str__(self):
        return str(self.id())

    def __repr__(self):
        return '<Event: id=%s>' % str(self.id())

    def raw_message(self):
        """!
        @brief Return the raw message that generated this Event.
        """
        return self.message

    def ephemeral(self):
        """!
        @brief Returns True if the message is unsuitable for persistence, False otherwise.
        """
        return len(self.message) == 0

    @staticmethod
    def factory(self, serialized):
        """!
        @brief Return a new object based on the serialized data.
        """
        return NotImplementedError()

    def id(self):
        """!
        @brief Return a unique ID that represents this event. This method
        SHOULD be implemented by subclasses.
        """
        return str(self._id)

    def timestamp(self):
        """!
        @brief Return the timestamp of the event. This method MUST be
        implemented by subclasses.
        @returns DateTime object
        """
        raise NotImplementedError()

    def get_processing_delay(self):
        """!
        @brief Returns a timedelta representing the time delay until the event
        can be processed.
        """
        return datetime.timedelta(0)


class ProductstatusBaseEvent(Event):
    """!
    @brief Productstatus events.
    """
    def id(self):
        """!
        @brief Return Productstatus' message id for this event.
        """
        return self.kwargs['id']

    def timestamp(self):
        """!
        @brief Return the modified timestamp of the Productstatus resource.
        """
        return self.kwargs['timestamp']

    def protocol_version(self):
        """!
        @brief Return the Kafka/Productstatus message protocol version.
        """
        return self.kwargs['protocol_version']

    @staticmethod
    def factory(message):
        """!
        @brief Given a Kafka message object, return a ProductstatusBaseEvent instance.
        """
        kwargs = {
            'id': message.message_id,
            'timestamp': dateutil.parser.parse(message.message_timestamp),
            'protocol_version': message.version,
        }

        if message.type == 'resource':
            class_ = ProductstatusResourceEvent
            data = message.uri
        elif message.type == 'heartbeat':
            class_ = ProductstatusHeartbeatEvent
            data = {}
        else:
            class_ = ProductstatusBaseEvent
            data = {}

        return class_(
            message,
            data,
            **kwargs
        )


class ProductstatusResourceEvent(ProductstatusBaseEvent):
    """!
    @brief Productstatus events of type 'resource'.
    """
    def object_version(self):
        """!
        @brief Return the Productstatus Resource object version.
        """
        if self.protocol_version() >= [1, 5, 0]:
            return self.message.object_version
        return 1


class ProductstatusLocalEvent(ProductstatusBaseEvent):
    """!
    @brief Productstatus events, generated locally and not on the Kafka message queue.
    """
    def id(self):
        """!
        Return an ID for this event.
        """
        return str(self._id)


class ProductstatusHeartbeatEvent(ProductstatusBaseEvent):
    """!
    @brief Productstatus heartbeat event, only signifying that the server is alive.
    """
    pass


class RPCEvent(Event):
    """!
    @brief RPC events.
    """

    def timestamp(self):
        """!
        @brief Return the timestamp of the object instantiation.
        """
        return self.kwargs['timestamp']
