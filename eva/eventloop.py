import os
import datetime
import dateutil.tz
import copy
import traceback

import eva
import eva.config
import eva.event
import eva.globe
import eva.mail.text
import eva.rpc
import eva.zk

import productstatus.exceptions


class Eventloop(eva.config.ConfigurableObject, eva.globe.GlobalMixin):
    """!
    The main loop.
    """

    CONFIG = {
        'EVA_CONCURRENCY': {
            'type': 'positive_int',
            'help': 'How many Executor tasks to run at the same time',
            'default': '1',
        },
        'EVA_QUEUE_ORDER': {
            'type': 'string',
            'help': 'Specify how to process incoming events; one of FIFO, LIFO, ADAPTIVE. See the documentation for implementation details',
            'default': 'FIFO',
        },
    }

    OPTIONAL_CONFIG = [
        'EVA_CONCURRENCY',
        'EVA_QUEUE_ORDER',
    ]

    RECOVERABLE_EXCEPTIONS = (eva.exceptions.RetryException, productstatus.exceptions.ServiceUnavailableException,)

    # Queue orders, used in Eventloop.sort_queue()
    QUEUE_ORDER_FIFO = 0
    QUEUE_ORDER_LIFO = 1
    QUEUE_ORDER_ADAPTIVE = 2

    QUEUE_ORDERS = {
        'FIFO': QUEUE_ORDER_FIFO,
        'LIFO': QUEUE_ORDER_LIFO,
        'ADAPTIVE': QUEUE_ORDER_ADAPTIVE,
    }

    # Allow maximum 60 seconds between each heartbeat before reporting process unhealthy
    HEALTH_CHECK_HEARTBEAT_TIMEOUT = 60

    def __init__(self,
                 globe,
                 productstatus_api,
                 listeners,
                 adapter,
                 executor,
                 environment_variables,
                 health_check_server,
                 ):
        self.globe = globe
        self.listeners = listeners
        self.productstatus_api = productstatus_api
        self.adapter = adapter
        self.executor = executor
        self.env = environment_variables
        self.health_check_server = health_check_server

        self.read_configuration()
        self.concurrency = self.env['EVA_CONCURRENCY']
        self.queue_order = self.parse_queue_order(self.env['EVA_QUEUE_ORDER'])
        self.drain = False
        self.event_queue = []
        self.process_list = []
        self.do_shutdown = False
        self.message_timestamp_threshold = datetime.datetime.fromtimestamp(0, dateutil.tz.tzutc())

        event_listener_configuration = self.productstatus_api.get_event_listener_configuration()
        if hasattr(event_listener_configuration, 'heartbeat_interval'):
            self.set_health_check_skip_heartbeat(False)
            self.set_health_check_heartbeat_interval(int(event_listener_configuration.heartbeat_interval))
            self.set_health_check_heartbeat_timeout(self.HEALTH_CHECK_HEARTBEAT_TIMEOUT)

    def parse_queue_order(self, s):
        """!
        @brief Parse a configuration string into a queue order.
        """
        s = s.upper()
        if s not in self.QUEUE_ORDERS:
            raise eva.exceptions.InvalidConfigurationException(
                'EVA_QUEUE_ORDER order must be one of: %s' %
                ', '.join(sorted(self.QUEUE_ORDERS.keys()))
            )
        return self.QUEUE_ORDERS[s]

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
                self.logger.debug('Received invalid event: %s', e)
                continue
            except self.RECOVERABLE_EXCEPTIONS as e:
                self.logger.warning('Exception while receiving event: %s', e)
                continue

            # Duplicate events in queue should not happen
            if event.id() in [x.id() for x in self.event_queue]:
                self.logger.warning('Event with id %s is already in the event queue. This is most probably due to a previous Kafka commit error. The message has been discarded.', event.id())
                continue
            if event.id() in [x.id() for x in self.process_list]:
                self.logger.warning('Event with id %s is already in the process list. This is most probably due to a previous Kafka commit error. The message has been discarded.', event.id())
                continue

            # Accept heartbeats without adding them to queue
            if isinstance(event, eva.event.ProductstatusHeartbeatEvent):
                listener.acknowledge()
                self.set_health_check_timestamp(eva.now_with_timezone())
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
        self.set_health_check_skip_heartbeat(True)
        for listener in self.listeners:
            listener.close_listener()

    def set_no_drain(self):
        """!
        @brief Define that new events from queues will again be accepted for
        processing. This will restart the message queue listener.
        """
        self.drain = False
        self.logger.info('Drain disabled. Will restart message listeners and start accepting new events.')
        self.set_health_check_skip_heartbeat(False)
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
        self.logger.debug('Storing %s in ZooKeeper, number of items: %d', log_name, len(raw))
        try:
            count, size = eva.zk.store_serialized_data(self.zookeeper, path, raw)
        except eva.exceptions.ZooKeeperDataTooLargeException as e:
            self.logger.warning(str(e))
            return False
        self.logger.debug('Successfully stored %d items with size %d', count, size)
        self.statsd.gauge(metric_base + '_count', count)
        self.statsd.gauge(metric_base + '_size', size)
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
        data = eva.zk.load_serialized_data(self.zookeeper, path)
        return [eva.event.ProductstatusBaseEvent.factory(productstatus.event.Message(x)) for x in data if x]

    def load_event_queue(self):
        """!
        @brief Load the event queue from ZooKeeper.
        """
        if not self.zookeeper:
            return True
        self.logger.info('Loading event queue from ZooKeeper.')
        self.event_queue = self.load_serialized_data(self.zookeeper_event_queue_path())

    def load_process_list(self):
        """!
        @brief Load the process list from ZooKeeper.
        """
        if not self.zookeeper:
            return True
        self.logger.info('Loading process list from ZooKeeper.')
        self.process_list = self.load_serialized_data(self.zookeeper_process_list_path())

    def move_to_process_list(self, event):
        """!
        @brief Move an event from the event queue to the process list.
        @returns True if the event was moved, False otherwise.
        """
        if event not in self.event_queue:
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

    def instantiate_productstatus_data(self, event):
        """!
        @brief Make sure a ProductstatusResourceEvent has a Productstatus resource in Event.data.
        """
        if isinstance(event.data, str) and type(event) == eva.event.ProductstatusResourceEvent:
            event.data = self.productstatus_api[event.data]

    def process_health_check(self):
        """!
        @brief Make sure health check requests are processed.
        """
        if self.health_check_server:
            self.health_check_server.respond_to_next_request()

    def set_health_check_skip_heartbeat(self, skip):
        """!
        @brief Tell the health check server to report healthy if heartbeats are skipped.
        """
        if self.health_check_server:
            self.logger.debug('Setting health check heartbeat skip: %s', str(skip))
            self.health_check_server.set_skip_heartbeat(bool(skip))

    def set_health_check_heartbeat_interval(self, interval):
        """!
        @brief Set the number of seconds expected between each heartbeat from the Productstatus message queue.
        """
        if self.health_check_server:
            self.logger.debug('Setting health check heartbeat interval to %d seconds', interval)
            self.health_check_server.set_heartbeat_interval(interval)

    def set_health_check_heartbeat_timeout(self, timeout):
        """!
        @brief Set the number of seconds expected between each heartbeat from the Productstatus message queue.
        """
        if self.health_check_server:
            self.logger.debug('Setting health check heartbeat timeout to %d seconds', timeout)
            self.health_check_server.set_heartbeat_timeout(timeout)

    def set_health_check_timestamp(self, timestamp):
        """!
        @brief Give a heartbeat to the health check server.
        """
        if self.health_check_server:
            self.logger.debug('Setting health check heartbeat at timestamp %s', timestamp)
            self.health_check_server.heartbeat(timestamp)

    def sort_queue(self):
        """!
        @brief Sort queue according to EVA_QUEUE_ORDER.

        This function guarantees that:

        * RPC requests are put first in the queue in FIFO order
        * If using the FIFO order, messages are put in chronological order
        * If using the LIFO order, messages are put in reverse chronological order
        * If using the ADAPTIVE order, messages are put in chronological order,
          but messages with a more recent reference time are put first in the queue.
        """
        def sort_timestamp(event):
            return event.timestamp()

        def sort_rpc(event):
            return not isinstance(event, eva.event.RPCEvent)

        def sort_reference_time(event):
            if not isinstance(event, eva.event.ProductstatusResourceEvent):
                return eva.epoch_with_timezone()
            self.instantiate_productstatus_data(event)
            if event.data._collection._resource_name != 'datainstance':
                return eva.epoch_with_timezone()
            return event.data.data.productinstance.reference_time

        if self.queue_order == self.QUEUE_ORDER_FIFO:
            self.event_queue.sort(key=sort_timestamp)
        elif self.queue_order == self.QUEUE_ORDER_LIFO:
            self.event_queue.sort(key=sort_timestamp, reverse=True)
        elif self.queue_order == self.QUEUE_ORDER_ADAPTIVE:
            self.event_queue.sort(key=sort_timestamp)
            self.event_queue.sort(key=sort_reference_time, reverse=True)
        self.event_queue.sort(key=sort_rpc)

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

    def register_job_failure(self, event):
        """!
        @brief Increase the number of failures for a specific event, for
        statistic and mail purposes.
        """
        failures = self.adapter.incr_processing_failures(event.id())
        self.logger.warning('Job %s failed, total fail count: %d.', event.id(), failures)

        # Only send mail on the first failure
        if failures != 1:
            return

        template_params = {
            'event_id': event.id(),
        }
        subject = eva.mail.text.JOB_FAIL_SUBJECT % template_params
        text = eva.mail.text.JOB_FAIL_TEXT % template_params

        self.mailer.send_email(subject, text)

    def register_job_success(self, event):
        """!
        @brief Set the number of failures for a specific event to zero, and
        send out an e-mail in case it recovered from a non-zero error count.
        """
        failures = self.adapter.processing_failures(event.id())
        self.adapter.set_processing_failures(event.id(), 0)

        # Skip sending mail for healthy jobs
        if failures == 0:
            return

        template_params = {
            'event_id': event.id(),
            'failures': failures,
        }
        subject = eva.mail.text.JOB_RECOVER_SUBJECT % template_params
        text = eva.mail.text.JOB_RECOVER_TEXT % template_params

        self.mailer.send_email(subject, text)

    def process_all_events_once(self):
        """!
        @brief Process any events in the process list once.
        @returns True if there is anything left to process, false otherwise.
        """
        self.process_health_check()
        self.fill_process_list()

        for index, event in enumerate(self.process_list):
            self.process_health_check()

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
                if hasattr(event, 'job'):
                    del event.job
                self.logger.error('Re-queueing failed event %s due to error: %s', event, e)
                self.statsd.incr('requeued_jobs')
                # reload event in order to get fresh Productstatus data
                if isinstance(event, eva.event.ProductstatusResourceEvent):
                    self.logger.info('Re-creating event from memory in order to reload Productstatus data.')
                    self.process_list[index] = event.factory(event.message)
                continue

        return not self.both_queues_empty()

    def event_matches_object_version(self, event):
        """!
        @brief Return True if Event.object_version() equals Resource.object_version, False otherwise.
        """
        if not isinstance(event, eva.event.ProductstatusResourceEvent):
            return False
        self.instantiate_productstatus_data(event)
        return event.object_version() == event.data.object_version

    def process_event(self, event):
        """!
        @brief Run asynchronous processing of an current event.

        This function will, based on the status of the event:

        * Ask the Adapter to initialize the Job
        * Send the Job for execution to the Executor
        * Send a finish message to the Adapter
        """

        # Checks for real Productstatus events from the message queue
        if type(event) is eva.event.ProductstatusResourceEvent:

            # Only process messages with the correct version
            if event.protocol_version()[0] != 1:
                self.logger.warning('Event version is %s, but I am only accepting major version 1. Discarding message.', '.'.join(event.protocol_version()))
                self.statsd.incr('event_version_unsupported')
                self.remove_event_from_queues(event)
                return

            # Discard messages that date from an earlier Resource version
            if not self.event_matches_object_version(event):
                self.logger.warning('Resource object version is %d, expecting it to be equal to the Event object version %d. The message is too old, discarding.', event.data.object_version, event.object_version())
                self.statsd.incr('resource_object_version_too_old')
                self.remove_event_from_queues(event)
                return

        # Create event job if it has not been created yet
        if not hasattr(event, 'job'):
            self.logger.debug('Start processing event: %s', event)
            self.instantiate_productstatus_data(event)
            resource = event.data
            if not self.adapter.validate_resource(resource):
                self.logger.debug('Adapter did not validate the current event, skipping.')
                self.statsd.incr('productstatus_rejected_events')
                self.remove_event_from_queues(event)
            else:
                self.logger.debug('Creating a Job object for the current event...')
                self.statsd.incr('productstatus_accepted_events')
                event.job = self.adapter.create_job(event.id(), resource)
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
            if not event.job.complete():
                self.register_job_failure(event)
            else:
                self.register_job_success(event)
            self.adapter.finish_job(event.job)
            try:
                self.adapter.generate_and_post_resources(event.job)
            except eva.exceptions.JobNotCompleteException as e:
                # ignore non-fatal errors
                event.job.logger.error(e)
                event.job.logger.warning('Job is not complete, skipping anyway.')
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
            backtrace = traceback.format_exc().split("\n")
            for line in backtrace:
                self.logger.critical(line)
        self.logger.info('Finished processing RPC request: %s', str(event))
        self.remove_event_from_queues(event)

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
