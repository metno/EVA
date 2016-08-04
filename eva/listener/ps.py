"""!
@brief Productstatus event listener. Note that the funny module name is because
of this module's potential collision with the real productstatus module,
hindering imports of the exception classes.
"""

import os
import time
import timeit
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
        self.event_queue = []
        self.event_listener = self.kwargs['productstatus_api'].get_event_listener(
            client_id=self.kwargs['client_id'],
            group_id=self.kwargs['group_id'],
            consumer_timeout_ms=10,
        )

    def event_path(self):
        """!
        @brief Return the ZooKeeper path to the store of cached messages.
        """
        return os.path.join(self.zookeeper.EVA_BASE_PATH, 'kafka_cached_messages')

    def get_stored_events(self):
        """!
        @brief Get the list of cached messages from ZooKeeper.
        """
        if not self.zookeeper:
            return self.event_queue
        try:
            serialized = self.zookeeper.get(self.event_path())
        except kazoo.exceptions.NoNodeError:
            return []
        return json.loads(serialized[0].decode('ascii'))

    def set_stored_events(self, events):
        """!
        @brief Get the list of cached messages from ZooKeeper.
        """
        if not self.zookeeper:
            self.event_queue = events
            return

        serialized = json.dumps(events).encode('ascii')
        path = self.event_path()
        self.logger.debug('Setting stored event list, number of items: %d', len(events))
        self.kwargs['statsd'].gauge('stored_event_list', len(events))

        if not self.zookeeper.exists(path):
            self.zookeeper.create(path, serialized)
        else:
            self.zookeeper.set(path, serialized)

    def get_next_stored_event(self):
        """!
        @brief Get the cached event from ZooKeeper. Returns None if the cache
        is empty.
        """
        events = self.get_stored_events()
        if len(events) == 0:
            return None
        return productstatus.event.Message(events[0])

    def delete_first_event(self):
        """!
        @brief Delete a cached event from the ZooKeeper cache.
        """
        events = self.get_stored_events()
        assert len(events) > 0
        self.logger.debug('Deleting first event from event cache: %s', events[0])
        self.set_stored_events(events[1:])

    def cache_queued_events(self):
        """!
        @brief Consume all pending messages on the Kafka queue, and store them in
        ZooKeeper for later processing.
        """
        # Merge new events into old events
        old_events = self.get_stored_events()
        events = []
        try:
            while True:
                event = self.event_listener.get_next_event()
                self.logger.debug('Productstatus message received: %s', event)
                events += [event]
        except productstatus.exceptions.EventTimeoutException:
            pass
        if len(events) == 0:
            return
        events = old_events + events
        self.set_stored_events(events)

        # Commit position to Kafka
        try:
            self.event_listener.save_position()
        except:
            self.logger.error('Unable to commit our current message queue position to Kafka. Trying to restore old message cache to ZooKeeper.')
            self.kwargs['statsd'].incr('error_kafka_commit')
            try:
                self.set_stored_events(old_events)
                self.logger.info('The old message cache has been restored. Raising original exception.')
            except:
                self.kwargs['statsd'].incr('error_cache_duplicates_queue')
                self.logger.critical('Old message cache could NOT be restored to ZooKeeper. This will result in duplicate message processing!')
            raise

    def get_next_event(self):
        """!
        @brief Return the next message on the Kafka topic sent by Productstatus.
        """
        try:
            self.cache_queued_events()
            event = self.get_next_stored_event()
            if not event:
                raise eva.exceptions.EventTimeoutException('No events in the event queue.')
            return eva.event.ProductstatusEvent(
                self.kwargs['productstatus_api'][event.uri],
                id=event.message_id,
                timestamp=dateutil.parser.parse(event.message_timestamp),
                event_listener=self.event_listener,
                parent=self,
            )
        except self.RECOVERABLE_EXCEPTIONS as e:
            raise eva.exceptions.EventTimeoutException(e)
