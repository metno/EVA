"""!
@brief Productstatus event listener. Note that the funny module name is because
of this module's potential collision with the real productstatus module,
hindering imports of the exception classes.
"""

import eva.event
import eva.base.listener

import productstatus.exceptions


class ProductstatusListener(eva.base.listener.BaseListener):
    """!
    @brief Listen for events from Productstatus.
    """

    RECOVERABLE_EXCEPTIONS = (
        productstatus.exceptions.EventTimeoutException,
        productstatus.exceptions.ServiceUnavailableException,
    )

    def setup_listener(self):
        """!
        @brief Instantiate Productstatus event listener.
        """
        self.event_listener = self.kwargs['productstatus_api'].get_event_listener(
            client_id=self.kwargs['client_id'],
            group_id=self.group_id,
            consumer_timeout_ms=10,
        )

    def close_listener(self):
        """!
        @brief Drop the connection to Kafka.
        """
        self.kwargs['productstatus_api'].delete_event_listener()
        del self.event_listener

    def get_next_event(self):
        """!
        @brief Return the next message on the Kafka topic sent by Productstatus.
        """
        try:
            event = self.event_listener.get_next_event()
            if not event:
                raise eva.exceptions.EventTimeoutException('No Productstatus messages available for consumption.')
            self.logger.debug('Productstatus message received: %s', event)
            return eva.event.ProductstatusBaseEvent.factory(event)
        except self.RECOVERABLE_EXCEPTIONS as e:
            raise eva.exceptions.EventTimeoutException(e)

    def acknowledge(self):
        """!
        @brief Store the current message offset in the Kafka queue.
        """
        self.logger.debug('Acknowledging current Kafka position.')
        self.event_listener.save_position()
