"""!
@brief Productstatus event listener. Note that the funny module name is because
of this module's potential collision with the real productstatus module,
hindering imports of the exception classes.
"""

import time
import dateutil.parser

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
            group_id=self.kwargs['group_id'],
            consumer_timeout_ms=1000,
        )

    def get_next_event(self):
        """!
        @brief Poll for Productstatus messages.
        """
        try:
            event = self.event_listener.get_next_event()
            self.logger.debug('Productstatus message received: %s', event)
            return eva.event.ProductstatusEvent(
                self.kwargs['productstatus_api'][event.uri],
                id=event.message_id,
                timestamp=dateutil.parser.parse(event.message_timestamp),
                event_listener=self.event_listener,
            )
        except self.RECOVERABLE_EXCEPTIONS as e:
            raise eva.exceptions.EventTimeoutException(e)
