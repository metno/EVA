import uuid
import datetime

import eva


class Event(object):
    """!
    @brief Base class for received events.
    """
    def __init__(self, data, **kwargs):
        self._id = uuid.uuid4()
        self.data = data
        self.kwargs = kwargs

    def __str__(self):
        return str(self.data)

    def __repr__(self):
        return str(self)

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

    def acknowledge(self):
        """!
        @brief This function is called from the event loop when an event has
        been successfully processed. This method MUST be implemented by subclasses.
        """
        raise NotImplementedError()

    def get_processing_delay(self):
        """!
        @brief Returns a timedelta representing the time delay until the event
        can be processed.
        """
        return datetime.timedelta(0)


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
        self.kwargs['parent'].delete_first_event()

    def timestamp(self):
        """!
        @brief Return the modified timestamp of the Productstatus resource.
        """
        return self.kwargs['timestamp']


class ProductstatusLocalEvent(ProductstatusEvent):
    """!
    @brief Productstatus events, generated locally and not on the Kafka message queue.
    """

    def id(self):
        return str(self._id)

    def acknowledge(self):
        """!
        @brief Fake message acknowledgement.
        """
        pass


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
