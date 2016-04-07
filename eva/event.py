import uuid


class Event(object):
    """!
    @brief Base class for received events.
    """
    def __init__(self, data, **kwargs):
        self.data = data
        self.kwargs = kwargs

    def __unicode__(self):
        return unicode(self.data)

    def __repr__(self):
        return unicode(self)

    def id(self):
        """!
        @brief Return a unique ID that represents this event. This method
        SHOULD be implemented by subclasses.
        """
        return uuid.uuid4()

    def timestamp(self):
        """!
        @brief Return the timestamp of the event. This method MUST be
        implemented by subclasses.
        @returns DateTime object
        """
        raise NotImplementedError()

    def acknowledge(self):
        """!
        @brief This function is called from the event loop when an event has
        been successfully processed. This method MUST be implemented by subclasses.
        """
        raise NotImplementedError()


class ProductstatusEvent(Event):
    """!
    @brief Productstatus events.
    """

    def id(self):
        """!
        @brief Return Productstatus' message id for this event.
        """
        return self.kwargs['id']

    def acknowledge(self):
        """!
        @brief Store message position in Kafka.
        """
        self.kwargs['event_listener'].save_position()

    def timestamp(self):
        """!
        @brief Return the modified timestamp of the Productstatus resource.
        """
        return self.kwargs['timestamp']


class RPCEvent(Event):
    """!
    @brief RPC events.
    """

    def acknowledge(self):
        """!
        @brief RPC messages are not acknowledged.
        """
        pass

    def timestamp(self):
        """!
        @brief Return the timestamp of the object instantiation.
        """
        return self.kwargs['timestamp']
