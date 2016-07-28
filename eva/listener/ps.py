"""!
@brief Productstatus event listener. Note that the funny module name is because
of this module's potential collision with the real productstatus module,
hindering imports of the exception classes.
"""

import os
import time
import json
import kazoo.exceptions
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

    def event_path(self):
        return os.path.join(self.zookeeper.EVA_BASE_PATH, 'current_event')

    def store_event(self, event):
        """!
        @brief Store the current event in ZooKeeper so that it is re-processed
        if EVA crashes or fails for some reason.
        """
        if not self.zookeeper:
            return
        self.logger.debug('Storing event in ZooKeeper.')
        serialized = json.dumps(event).encode('ascii')
        self.zookeeper.create(self.event_path(), serialized)

    def get_stored_event(self):
        """!
        @brief Get the cached event from ZooKeeper. Returns None if the cache
        is empty.
        """
        if not self.zookeeper:
            return
        try:
            serialized = self.zookeeper.get(self.event_path())
        except kazoo.exceptions.NoNodeError:
            return None
        return productstatus.event.Message(json.loads(serialized[0].decode('ascii')))

    def delete_stored_event(self):
        """!
        @brief Delete a cached event from the ZooKeeper cache.
        """
        if not self.zookeeper:
            return
        try:
            self.zookeeper.delete(self.event_path())
        except kazoo.exceptions.NoNodeError:
            pass

    def get_next_event(self):
        """!
        @brief Poll for Productstatus messages.
        """
        try:
            event = self.get_stored_event()
            if not event:
                event = self.event_listener.get_next_event()
                self.logger.debug('Productstatus message received: %s', event)
                self.store_event(event)
                self.event_listener.save_position()
            else:
                self.logger.warning('Using cached Productstatus message: %s', event)
            return eva.event.ProductstatusEvent(
                self.kwargs['productstatus_api'][event.uri],
                id=event.message_id,
                timestamp=dateutil.parser.parse(event.message_timestamp),
                event_listener=self.event_listener,
                parent=self,
            )
        except self.RECOVERABLE_EXCEPTIONS as e:
            raise eva.exceptions.EventTimeoutException(e)
