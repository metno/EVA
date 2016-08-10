import os
import json
import datetime
import dateutil.tz
import copy
import time
import uuid

import eva
import eva.rpc
import eva.event

import productstatus.exceptions


# Max message size in ZooKeeper can be safely assumed to be 1MB.
ZOOKEEPER_MSG_LIMIT = 1024**2


class Eventloop(object):
    """!
    The main loop.
    """

    RECOVERABLE_EXCEPTIONS = (eva.exceptions.RetryException, productstatus.exceptions.ServiceUnavailableException,)

    def __init__(self,
                 productstatus_api,
                 listeners,
                 adapter,
                 executor,
                 statsd,
                 zookeeper,
                 concurrency,
                 environment_variables,
                 logger,
                 ):
        self.listeners = listeners
        self.productstatus_api = productstatus_api
        self.adapter = adapter
        self.executor = executor
        self.statsd = statsd
        self.zookeeper = zookeeper
        self.concurrency = concurrency
        self.env = environment_variables
        self.logger = logger

        self.drain = False
        self.event_queue = []
        self.process_list = []
        self.do_shutdown = False
        self.message_timestamp_threshold = datetime.datetime.fromtimestamp(0, dateutil.tz.tzutc())

    def poll_listeners(self):
        """!
        @brief Poll for new messages from all message listeners.
        """
        for listener in self.listeners:
            try:
                event = listener.get_next_event(self.adapter.resource_matches_input_config)
                assert isinstance(event, eva.event.Event)
            except eva.exceptions.EventTimeoutException:
                continue
            except eva.exceptions.InvalidEventException as e:
                self.logger.warning('Received invalid event: %s', e)
                continue
            except self.RECOVERABLE_EXCEPTIONS as e:
                self.logger.warning('Exception while receiving event: %s', e)
                continue

            # Duplicate events in queue should not happen
            if event.id() in [x.id() for x in self.event_queue]:
                self.logger.warning('Event with id %s is already in the event queue. This is most probably due to a previous Kafka commit error. The message has been discarded.')
                continue
            if event.id() in [x.id() for x in self.process_list]:
                self.logger.warning('Event with id %s is already in the process list. This is most probably due to a previous Kafka commit error. The message has been discarded.')
                continue

            if self.add_event_to_queue(event):
                listener.acknowledge()

    def event_queue_empty(self):
        """!
        @returns True if the event queue is empty, False otherwise.
        """
        return len(self.event_queue) == 0

    def process_list_empty(self):
        """!
        @returns True if the process list is empty, False otherwise.
        """
        return len(self.process_list) == 0

    def both_queues_empty(self):
        """!
        @returns True if the event queue and process lists are both empty, False otherwise.
        """
        return self.event_queue_empty() and self.process_list_empty()

    def set_drain(self):
        """!
        @brief Define that new events from queues will not be accepted for processing.
        """
        self.drain = True
        self.logger.warning('Drain enabled! Will NOT process any more events from message listeners until event queue is empty!')
        for listener in self.listeners:
            listener.close_listener()

    def set_no_drain(self):
        """!
        @brief Define that new events from queues will again be accepted for
        processing. This will restart the message queue listener.
        """
        self.drain = False
        self.logger.info('Drain disabled. Will restart message listeners and start accepting new events.')
        for listener in self.listeners:
            listener.setup_listener()

    def draining(self):
        """!
        @brief Returns True if event queue draining is enabled, False otherwise.
        """
        return self.drain is True

    def drained(self):
        """!
        @returns True if EVA is draining queues for messages AND both process queues are empty.
        """
        return self.draining() and self.both_queues_empty()

    def process_list_full(self):
        """!
        @returns True if the process list is empty, False otherwise.
        """
        return len(self.process_list) >= self.concurrency

    def add_event_to_queue(self, event):
        """!
        @brief Add an event to the event queue.
        @returns True if the event was successfully added, False otherwise.
        """
        assert isinstance(event, eva.event.Event)
        self.event_queue += [event]
        self.logger.debug('Adding event to queue: %s', event)
        if self.store_event_queue():
            self.logger.debug('Event added to queue: %s', event)
            return True
        else:
            self.logger.debug('Event could not be added to queue: %s', event)
            self.set_drain()
            self.event_queue = self.event_queue[:-1]
            return False

    def zookeeper_path(self, *args):
        """!
        @brief Return a ZooKeeper path.
        """
        return os.path.join(self.zookeeper.EVA_BASE_PATH, *args)

    def zookeeper_event_queue_path(self):
        """!
        @brief Return the ZooKeeper path to the store of cached event queue messages.
        """
        return self.zookeeper_path('event_queue')

    def zookeeper_process_list_path(self):
        """!
        @brief Return the ZooKeeper path to the store of process list messages.
        """
        return self.zookeeper_path('process_list')

    def store_serialized_data(self, path, data, metric_base, log_name):
        """!
        @brief Store structured data in ZooKeeper.
        @returns True if the data could be stored in ZooKeeper, False otherwise.
        @throws kazoo.exceptions.ZooKeeperError on failure
        """
        if not self.zookeeper:
            return True
        raw = [x.raw_message() for x in data if x is not None]
        serialized = json.dumps(raw).encode('ascii')
        serialized_byte_size = len(serialized)
        self.logger.debug('Storing %s in ZooKeeper, number of items: %d, size in bytes: %d', log_name, len(raw), serialized_byte_size)
        if serialized_byte_size > ZOOKEEPER_MSG_LIMIT:
            self.logger.warning('Cannot store %s in ZooKeeper since it exceeds the message limit of %d bytes.', log_name, ZOOKEEPER_MSG_LIMIT)
            return False
        if not self.zookeeper.exists(path):
            self.zookeeper.create(path, serialized)
        else:
            self.zookeeper.set(path, serialized)
        self.statsd.gauge(metric_base + '_count', len(raw))
        self.statsd.gauge(metric_base + '_size', serialized_byte_size)
        return True

    def store_event_queue(self):
        """!
        @brief Store the event queue in ZooKeeper.
        @returns True if the data could be stored in ZooKeeper, False otherwise.
        @throws kazoo.exceptions.ZooKeeperError on failure
        """
        if not self.zookeeper:
            return True
        return self.store_serialized_data(self.zookeeper_event_queue_path(), self.event_queue, 'event_queue', 'event queue')

    def store_process_list(self):
        """!
        @brief Store the event processing list in ZooKeeper.
        @returns True if the data could be stored in ZooKeeper, False otherwise.
        @throws kazoo.exceptions.ZooKeeperError on failure
        """
        if not self.zookeeper:
            return True
        return self.store_serialized_data(self.zookeeper_process_list_path(), self.process_list, 'process_list', 'process list')

    def load_serialized_data(self, path):
        """!
        @brief Load the stored event queue from ZooKeeper.
        @returns The loaded data.
        """
        if not self.zookeeper:
            return []
        if self.zookeeper.exists(path):
            serialized = self.zookeeper.get(path)
            return [eva.event.ProductstatusEvent.factory(productstatus.event.Message(x)) for x in json.loads(serialized[0].decode('ascii'))]
        return []

    def load_event_queue(self):
        """!
        @brief Load the event queue from ZooKeeper.
        """
        self.logger.info('Loading event queue from ZooKeeper.')
        self.event_queue = self.load_serialized_data(self.zookeeper_event_queue_path())

    def load_process_list(self):
        """!
        @brief Load the process list from ZooKeeper.
        """
        self.logger.info('Loading process list from ZooKeeper.')
        self.process_list = self.load_serialized_data(self.zookeeper_process_list_path())

    def move_to_process_list(self, event):
        """!
        @brief Move an event from the event queue to the process list.
        @returns True if the event was moved, False otherwise.
        """
        if not event in self.event_queue:
            return False
        self.process_list += [event]
        self.event_queue.remove(event)
        self.store_process_list()
        self.store_event_queue()

    def remove_event_from_queues(self, event):
        """!
        @brief Remove an event from the event queue and the process list.
        """
        if event in self.event_queue:
            self.event_queue.remove(event)
            self.store_event_queue()
        if event in self.process_list:
            self.process_list.remove(event)
            self.store_process_list()

    def sort_queue(self):
        """!
        @brief Ensure that RPC requests are moved to the top of the queue.
        """
        self.event_queue.sort(key=lambda event: not isinstance(event, eva.event.RPCEvent))

    def __call__(self):
        """!
        @brief Main loop. Checks for Productstatus events and dispatchs them to the adapter.
        """
        self.logger.info('Start processing events and RPC calls.')
        self.load_process_list()
        self.load_event_queue()
        while not self.do_shutdown:
            if self.drained():
                self.set_no_drain()
            if not self.draining():
                timer = self.statsd.timer('poll_listeners')
                timer.start()
                self.poll_listeners()
                timer.stop()
            self.sort_queue()
            self.process_all_events_once()
        self.logger.info('Stop processing events and RPC calls.')

    def fill_process_list(self):
        """!
        @brief Iteration of the main loop. Fills the processing list with events from the event queue.
        @returns True if some events were moved into the other queue, False otherwise.
        """
        added = False
        while not self.event_queue_empty():
            if self.process_list_full():
                return added
            for event in self.event_queue:
                # Discard message if below timestamp threshold
                if event.timestamp() < self.message_timestamp_threshold:
                    self.logger.warning('Skip processing event because resource is older than threshold: %s vs %s',
                                        event.timestamp(),
                                        self.message_timestamp_threshold)
                    self.remove_event_from_queues(event)
                    self.statsd.incr('productstatus_rejected_events')
                else:
                    self.move_to_process_list(event)
                    added = True
                break
        return added

    def process_all_events_once(self):
        """!
        @brief Process any events in the process list once.
        @returns True if there is anything left to process, false otherwise.
        """
        self.fill_process_list()
        for event in self.process_list:
            if isinstance(event, eva.event.RPCEvent):
                event.data.set_executor_instance(self)
                self.process_rpc_event(event)
                continue

                delay = event.get_processing_delay().total_seconds()
                if delay > 0:
                    self.logger.info('Postponing processing of event due to %.1f seconds event delay', delay)
                    continue

            try:
                self.process_event(event)
            except self.RECOVERABLE_EXCEPTIONS as e:
                del self.current_event.job
                self.logger.error('Re-queueing failed event %s due to error: %s', (event, e))
                self.statsd.incr('requeued_jobs')
                continue

        return not self.both_queues_empty()

    def process_event(self, event):
        """!
        @brief Run asynchronous processing of an current event.

        This function will, based on the status of the event:

        * Ask the Adapter to initialize the Job
        * Send the Job for execution to the Executor
        * Send a finish message to the Adapter
        """

        # Create event job if it has not been created yet
        if not hasattr(event, 'job'):
            self.logger.debug('Start processing event: %s', event)
            if isinstance(event.data, productstatus.api.Resource):
                resource = event.data
            else:
                resource = self.productstatus_api[event.data]
            if not self.adapter.validate_resource(resource):
                self.logger.debug('Adapter did not validate the current event, skipping.')
                self.statsd.incr('productstatus_rejected_events')
                self.remove_event_from_queues(event)
            else:
                self.logger.debug('Creating a Job object for the current event...')
                self.statsd.incr('productstatus_accepted_events')
                event.job = self.adapter.create_job(event.id(), event.data)
                if not event.job:
                    self.logger.debug('No Job object was returned by the adapter; assuming no-op.')
                    self.remove_event_from_queues(event)
                else:
                    event.job.resource = resource
                    event.job.timer = self.statsd.timer('execution_time')
                    event.job.logger.info('Created Job object for event: %s', event)

        # Start job if it is not running
        elif event.job.initialized():
            event.job.logger.info('Sending job to executor for asynchronous execution...')
            event.job.timer.start()
            self.executor.execute_async(event.job)
            event.job.logger.info('Job has been sent successfully to the executor.')

        # Check status of the job
        elif event.job.started():
            if event.job.poll_time_reached():
                event.job.logger.debug('Job is running, polling executor for job status...')
                self.executor.sync(event.job)
                event.job.logger.debug('Finished polling executor for job status.')

        # Tell adapter that the job has finished
        elif event.job.complete() or event.job.failed():
            event.job.timer.stop()
            event.job.logger.info('Finished with total time %.1fs; sending to adapter for finishing.', event.job.timer.total_time_msec() / 1000.0)
            self.adapter.finish_job(event.job)
            event.job.logger.info('Adapter has finished processing the job.')
            self.remove_event_from_queues(event)
            self.logger.debug('Finished processing event: %s', event)

    def process_rpc_event(self, event):
        """!
        @brief Process the latest RPC message in the RPC queue.
        """
        self.logger.info('Processing RPC request: %s', str(event))
        try:
            event.data()
        except eva.exceptions.RPCFailedException as e:
            self.logger.error('Error while executing RPC request: %s', e)
        self.logger.info('Finished processing RPC request: %s', str(event))

    def set_message_timestamp_threshold(self, timestamp):
        """!
        @brief Fast-forward the message queue to a specific time.
        """
        ts = copy.copy(timestamp)
        if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
            self.logger.warning('Received a naive datetime string, assuming UTC')
            ts = ts.replace(tzinfo=dateutil.tz.tzutc())
        self.message_timestamp_threshold = copy.copy(ts)
        self.logger.info('Forwarding message queue threshold timestamp to %s', self.message_timestamp_threshold)

    def process_all_in_product_instance(self, product_instance):
        """!
        @brief Process all child DataInstance objects of a ProductInstance.
        """
        events = []
        self.logger.info('Processing all DataInstance resources descended from %s', product_instance)
        try:
            instances = self.productstatus_api.datainstance.objects.filter(data__productinstance=product_instance).order_by('created')
            index = 1
            count = instances.count()
            self.logger.info('Adding %d DataInstance resources to queue...', count)
            for resource in instances:
                self.logger.info('[%d/%d] Adding to queue: %s', index, count, resource)
                events += [eva.event.ProductstatusLocalEvent(
                    {},
                    resource,
                    timestamp=resource.modified,
                )]
                index += 1
        except self.RECOVERABLE_EXCEPTIONS as e:
            self.logger.error('An error occurred when retrieving Productstatus resources, aborting: %s', e)
            return
        [self.add_event_to_queue(x) for x in events]

    def process_data_instance(self, data_instance_uuid):
        """!
        @brief Process a single DataInstance resource.
        """
        resource = self.productstatus_api.datainstance[data_instance_uuid]
        event = eva.event.ProductstatusLocalEvent(
            {},
            resource,
            timestamp=resource.modified,
        )
        self.logger.info('Adding event with DataInstance %s to queue', resource)
        self.add_event_to_queue(event)

    def blacklist_uuid(self, uuid):
        """!
        @brief Omit processing a specific DataInstance for the lifetime of this EVA process.
        """
        self.adapter.blacklist_add(uuid)

    def forward_to_uuid(self, uuid):
        """!
        @brief Skip all Productstatus messages where parents or children do not
        contain the specified UUID. That includes Product, ProductInstance,
        Data, DataInstance, ServiceBackend and Format resources.
        """
        self.adapter.forward_to_uuid(uuid)

    def shutdown(self):
        """!
        @brief Shutdown EVA after the current resource has been processed.
        """
        self.logger.info('Received shutdown call, will stop processing resources.')
        self.do_shutdown = True
