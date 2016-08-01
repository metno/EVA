import datetime
import dateutil.tz
import copy
import time
import uuid

import eva
import eva.rpc
import eva.event

import productstatus.exceptions


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
                 environment_variables,
                 logger,
                 ):
        self.listeners = listeners
        self.productstatus_api = productstatus_api
        self.adapter = adapter
        self.executor = executor
        self.statsd = statsd
        self.env = environment_variables
        self.logger = logger
        self.current_event = None
        self.event_queue = []
        self.job_queue = []
        self.do_shutdown = False
        self.message_timestamp_threshold = datetime.datetime.fromtimestamp(0, dateutil.tz.tzutc())

    def poll_listeners(self):
        """!
        @brief Poll for new messages from all message listeners.
        """
        for listener in self.listeners:
            try:
                event = listener.get_next_event()
                assert isinstance(event, eva.event.Event)
            except eva.exceptions.EventTimeoutException:
                continue
            except eva.exceptions.InvalidEventException as e:
                self.logger.warning('Received invalid event: %s', e)
                continue
            # Ignore events already in queue
            if event.id() in [x.id() for x in self.event_queue]:
                continue
            if self.has_current_event() and event.id() == self.current_event.id():
                continue
            self.add_event_to_queue(event)

    def queue_empty(self):
        """!
        @returns True if the event queue is empty, False otherwise.
        """
        return len(self.event_queue) == 0

    def add_event_to_queue(self, event):
        """!
        @brief Add an event to the event queue.
        """
        self.event_queue += [event]
        self.logger.debug('Added event to queue: %s', event)

    def has_current_event(self):
        """!
        @brief Returns True if there is an event being processed at the moment.
        """
        return bool(self.current_event)

    def set_current_event(self, event):
        """!
        @brief Define that an event should be processed.
        """
        self.current_event = event
        self.logger.debug('Defining event to be processed: %s', self.current_event)

    def set_finished_current_event(self):
        """!
        @brief Set that the currently processed event has finished.
        """
        self.current_event.acknowledge()
        self.current_event = None
        self.logger.debug('Removed currently processed event.')

    def add_first_in_queue_to_processing(self):
        """!
        @brief Define that the first queued event should be processed.
        """
        assert not self.has_current_event()

        self.logger.debug('Event queue has %d pending events, adding the first one to processing...', len(self.event_queue))
        event = self.event_queue[0]
        self.shift_queue()

        # Discard message if below timestamp threshold
        if event.timestamp() < self.message_timestamp_threshold:
            self.logger.warning('Skip processing event because resource is older than threshold: %s vs %s',
                                event.timestamp(),
                                self.message_timestamp_threshold)
            return

        if isinstance(event, eva.event.RPCEvent):
            event.data.set_executor_instance(self)
            self.process_rpc_event(event)
        else:
            delay = event.get_processing_delay().total_seconds()
            if delay > 0:
                self.logger.info('Sleeping %.1f seconds before processing next event due to event delay',
                                 delay)
                time.sleep(delay)
            self.set_current_event(event)

    def sort_queue(self):
        """!
        @brief Ensure that RPC requests are moved to the top of the queue.
        """
        self.event_queue.sort(key=lambda event: not isinstance(event, eva.event.RPCEvent))

    def shift_queue(self):
        """!
        @brief Delete the first event in the event queue.
        """
        if not self.queue_empty():
            del self.event_queue[0]

    def __call__(self):
        """!
        @brief Main loop. Checks for Productstatus events and dispatchs them to the adapter.
        """
        self.logger.info('Start processing events and RPC calls.')
        while not self.do_shutdown:
            self.poll_listeners()
            self.sort_queue()
            self.process_all_events_once()
        self.logger.info('Stop processing events and RPC calls.')

    def process_all_events_once(self):
        """!
        @brief Iteration of the main loop.
        @returns True if there are more events ready for processing right away, False otherwise.
        """
        if self.has_current_event():
            try:
                self.process_current_event()
            except self.RECOVERABLE_EXCEPTIONS as e:
                del self.current_event.job
                self.logger.error('Restarting processing of event in 2 seconds due to error: %s', e)
                time.sleep(2.0)
        elif not self.queue_empty():
            self.add_first_in_queue_to_processing()
        return self.has_current_event() or not self.queue_empty()

    def process_current_event(self):
        """!
        @brief Run asynchronous processing of the current event.

        This function will, based on the status of the event:

        * Ask the Adapter to initialize the Job
        * Send the Job for execution to the Executor
        * Send a finish message to the Adapter
        """
        if not self.has_current_event():
            return

        event = self.current_event
        self.logger.debug('Start processing event: %s', event)

        # Create event job if it has not been created yet
        if not hasattr(event, 'job'):
            resource = event.data
            if not self.adapter.validate_resource(resource):
                self.logger.debug('Adapter did not validate the current event, skipping.')
                self.set_finished_current_event()
            else:
                self.logger.debug('Creating a Job object for the current event...')
                event.job = self.adapter.create_job(event.id(), event.data)
                if not event.job:
                    self.logger.debug('No Job object was returned by the adapter; assuming no-op.')
                    self.set_finished_current_event()
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
            else:
                event.job.logger.debug('Will not poll job for status until %s.', eva.strftime_iso8601(event.job.next_poll_time))

        # Tell adapter that the job has finished
        elif event.job.complete() or event.job.failed():
            event.job.timer.stop()
            event.job.logger.info('Finished with total time %.1fs; sending to adapter for finishing.', event.job.timer.total_time_msec() / 1000.0)
            self.adapter.finish_job(event.job)
            event.job.logger.info('Adapter has finished processing the job.')
            self.set_finished_current_event()

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
        event.acknowledge()
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
