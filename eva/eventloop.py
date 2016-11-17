import datetime
import dateutil.tz
import copy
import traceback

import eva
import eva.config
import eva.event
import eva.eventqueue
import eva.exceptions
import eva.globe
import eva.mail.text
import eva.rpc
import eva.zk

import productstatus.exceptions


class Eventloop(eva.globe.GlobalMixin):
    """!
    @brief Main EVA scheduler.

    This class is responsible for running the main loop, which is receiving
    events, maintaining an event queue, and scheduling jobs.
    """

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

    # Allow maximum 60 seconds since last heartbeat before reporting process unhealthy
    HEALTH_CHECK_HEARTBEAT_TIMEOUT = 60

    def __init__(self,
                 adapters,
                 listeners,
                 health_check_server,
                 ):
        self.adapters = adapters
        self.listeners = listeners
        self.health_check_server = health_check_server

    def init(self):
        #self.queue_order = self.parse_queue_order(self.env['EVA_QUEUE_ORDER'])
        self.drain = False
        self.event_queue = eva.eventqueue.EventQueue()
        self.event_queue.set_globe(self.globe)
        self.event_queue.init()
        self.do_shutdown = False
        self.message_timestamp_threshold = datetime.datetime.fromtimestamp(0, dateutil.tz.tzutc())

        event_listener_configuration = self.productstatus.get_event_listener_configuration()
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
        timer = self.statsd.timer('eva_poll_listeners')
        timer.start()

        for listener in self.listeners:
            try:
                event = listener.get_next_event()
                assert isinstance(event, eva.event.Event)
            except eva.exceptions.EventTimeoutException:
                continue
            except eva.exceptions.InvalidEventException as e:
                self.logger.debug('Received invalid event: %s', e)
                continue
            except self.RECOVERABLE_EXCEPTIONS as e:
                self.logger.warning('Exception while receiving event: %s', e)
                continue

            self.statsd.incr('eva_event_received')

            # Accept heartbeats without adding them to queue
            if isinstance(event, eva.event.ProductstatusHeartbeatEvent):
                listener.acknowledge()
                self.statsd.incr('eva_event_heartbeat')
                self.set_health_check_timestamp(eva.now_with_timezone())
                continue

            # Reject messages that are too old
            if event.timestamp() < self.message_timestamp_threshold:
                listener.acknowledge()
                self.statsd.incr('eva_event_too_old')
                self.logger.warning('Skip processing event because resource is older than threshold: %s vs %s',
                                    event.timestamp(),
                                    self.message_timestamp_threshold)

            # Checks for real Productstatus events from the message queue
            if type(event) is eva.event.ProductstatusResourceEvent:

                self.statsd.incr('eva_event_productstatus')

                # Make sure we get a Productstatus object from this resource
                eva.retry_n(self.instantiate_productstatus_data,
                            args=[event],
                            exceptions=self.RECOVERABLE_EXCEPTIONS,
                            give_up=0,
                            logger=self.logger)

                # Only process messages with the correct version
                if event.protocol_version()[0] != 1:
                    self.logger.warning('Event version is %s, but I am only accepting major version 1. Discarding message.', '.'.join(event.protocol_version()))
                    self.statsd.incr('eva_event_version_unsupported')
                    listener.acknowledge()
                    continue

                # Discard messages that date from an earlier Resource version
                if not self.event_matches_object_version(event):
                    self.logger.warning('Resource object version is %d, expecting it to be equal to the Event object version %d. The message is too old, discarding.', event.resource.object_version, event.object_version())
                    self.statsd.incr('eva_resource_object_version_too_old')
                    listener.acknowledge()
                    continue

            # Add message to event queue
            try:
                self.event_queue.add_event(event)
            except eva.exceptions.DuplicateEventException as e:
                self.statsd.incr('eva_event_duplicate')
                self.logger.warning(e)
                self.logger.warning('This is most probably due to a previous Kafka commit error. The message has been discarded.')

            listener.acknowledge()

        timer.stop()

    def remove_finished_events(self):
        """!
        @brief Process any events in the process list once.
        @returns True if there is anything left to process, false otherwise.
        """
        finished = []
        for item in self.event_queue:
            if item.finished():
                finished += [item]
        if not finished:
            return
        self.logger.debug('Removing finished events from queue...')
        for item in finished:
            self.logger.debug('Removing: %s', item)
            self.event_queue.remove_item(item)
        self.logger.debug('Finished removing finished events from queue.')

    def process_all_events_once(self):
        """!
        @brief Process any events in the process list once.
        @returns True if there is anything left to process, false otherwise.
        """
        for item in self.event_queue:

            # Answer health check requests
            self.process_health_check()

            # Ask all adapters to create jobs for this event
            if item.empty():
                jobs = self.create_jobs_for_event_queue_item(item)
                if not jobs:
                    self.logger.debug('No jobs generated for %s, discarding.', item)
                    self.event_queue.remove_item(item)
                    continue
                for job in jobs:
                    self.logger.debug('Adding job %s to event queue item %s', job, item)
                    item.add_job(job)
                self.event_queue.store_item(item)

            # Check if any jobs for this event has failed, and recreate them if necessary
            failed_jobs = item.failed_jobs()
            if failed_jobs:
                jobs = []
                for failed_job in item.failed_jobs():
                    jobs += [self.create_job_for_event_queue_item(self, item, failed_job.adapter)]
                    self.logger.debug('Removing failed job: %s', failed_job)
                    item.remove_job(failed_job)
                for job in jobs:
                    self.logger.debug('Re-queueing failed job replacement: %s', job)
                    item.add_job(job)
                    self.statsd.incr('eva_requeued_jobs')
                self.event_queue.store_item(item)

            # Postpone processing of event if it has a delay
            delay = item.event.get_processing_delay().total_seconds()
            if delay > 0:
                self.logger.info('Postponing processing of event due to %.1f seconds event delay', delay)
                continue

            # Process RPC events
            if isinstance(item.event, eva.event.RPCEvent):
                item.event.data.set_executor_instance(self)
                self.process_rpc_event(item.event)
                continue

            for job in item:
                try:
                    # Only process N active jobs at a time
                    if (not job.initialized()) or job.adapter.concurrency > self.event_queue.adapter_active_job_count(job.adapter):
                        self.process_job(job)
                except self.RECOVERABLE_EXCEPTIONS as e:
                    self.logger.error('Re-queueing failed event %s due to error: %s', item.event, e)
                    # reload event data in order to get fresh Productstatus data
                    del item.resource
                    self.instantiate_productstatus_data(item.event)

        return not self.event_queue.empty()

    def process_job(self, job):
        """!
        @brief Run asynchronous processing of an event queue item.

        This function will, based on the status of the event:

        * Ask the Adapter to initialize the Job
        * Send the Job for execution to the Executor
        * Send a finish message to the Adapter
        """

        # Start job if it is not running
        if job.initialized():
            job.logger.info('Sending job to executor for asynchronous execution...')
            job.timer.start()
            job.adapter.executor.execute_async(job)
            job.logger.info('Job has been sent successfully to the executor.')

        # Check status of the job
        elif job.started():
            if not job.poll_time_reached():
                return
            job.logger.debug('Job is running, polling executor for job status...')
            job.adapter.executor.sync(job)
            job.logger.debug('Finished polling executor for job status.')

        # Tell adapter that the job has finished
        elif job.complete() or job.failed():
            job.timer.stop()
            job.logger.info('Finished with total time %.1fs; sending to adapter for finishing.', job.timer.total_time_msec() / 1000.0)
            #if not job.complete():
                #self.register_job_failure(job)
            #else:
                #self.register_job_success(job)
            job.adapter.finish_job(job)
            try:
                job.adapter.generate_and_post_resources(job)
            except eva.exceptions.JobNotCompleteException as e:
                # ignore non-fatal errors
                job.logger.error(e)
                job.logger.warning('Job is not complete, skipping anyway.')
            job.logger.info('Adapter has finished processing the job.')
            job.set_status(eva.job.FINISHED)
            #self.remove_event_from_queues(event)

    def __call__(self):
        """!
        @brief Main loop. Checks for Productstatus events and dispatchs them to the adapter.
        """
        self.logger.info('Entering main loop.')
        #self.load_process_list()
        #self.load_event_queue()
        while not self.do_shutdown:
            if self.drained():
                self.set_no_drain()
            if not self.draining():
                self.poll_listeners()
            #self.sort_queue()
            self.process_health_check()
            self.process_all_events_once()
            self.store_job_status_change()
            self.report_job_status_metrics()
            self.remove_finished_events()
        self.logger.info('Exited main loop.')

    def report_job_status_metrics(self):
        """!
        @brief Report job status metrics to statsd.
        """
        status_count = self.event_queue.status_count()
        #strs = ['%s=%d' % (status, count) for status, count in status_count.items()]
        #self.logger.debug(' '.join(strs))
        for status, count in status_count.items():
            self.statsd.gauge('eva_job_status_count', count, tags={'status': status})

    def store_job_status_change(self):
        """!
        @brief Check if any job statuses have changed, and store the new
        statuses in the event queue's ZooKeeper instance.
        """
        changed = []
        for item in self.event_queue:
            for job in item:
                if job.status_changed():
                    changed += [item]
        for item in set(changed):
            self.event_queue.store_item(item)
            self.statsd.incr('eva_job_status_change')

    def create_job_for_event_queue_item(self, item, adapter):
        """!
        @brief Given an EventQueueItem object, create a job based on its Event.
        @returns eva.job.Job
        """
        resource = self.productstatus[item.event.data]

        if not adapter.validate_resource(resource):
            return None

        id = item.event.id() + '.' + adapter.config_id
        job = adapter.create_job(id, resource)

        if not job:
            return None

        job.resource = resource
        job.timer = self.statsd.timer('eva_execution_time')
        job.logger.info('Created Job object: %s', job)
        job.adapter = adapter

        return job

    def create_jobs_for_event_queue_item(self, item):
        """!
        @brief Given an EventQueueItem object, run through all adapters and
        create jobs based on the Event.
        @returns List of Job objects
        """
        jobs = []

        self.logger.debug('Start generating jobs for %s', item)

        for adapter in self.adapters:
            job = self.create_job_for_event_queue_item(item, adapter)

            if job is None:
                self.logger.debug('Adapter did not generate a job: %s', adapter.config_id)
                self.statsd.incr('eva_event_rejected', tags={'adapter': adapter.config_id})
                continue

            self.logger.debug('Adapter accepted event: %s', adapter.config_id)
            self.statsd.incr('eva_event_accepted', tags={'adapter': adapter.config_id})

            jobs += [job]

        return jobs

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
        @returns True if EVA is draining queues for messages AND event queue is empty.
        """
        return self.draining() and self.event_queue.empty()

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

    def zookeeper_process_list_path(self):
        """!
        @brief Return the ZooKeeper path to the store of process list messages.
        """
        return self.zookeeper_path('process_list')

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
        if type(event) is not eva.event.ProductstatusResourceEvent:
            return None
        event.resource = self.productstatus[event.data]

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
            #self.instantiate_productstatus_data(event)
            if event.resource._collection._resource_name != 'datainstance':
                return eva.epoch_with_timezone()
            return event.resource.data.productinstance.reference_time

        #if self.queue_order == self.QUEUE_ORDER_FIFO:
            #self.event_queue.sort(key=sort_timestamp)
        #elif self.queue_order == self.QUEUE_ORDER_LIFO:
            #self.event_queue.sort(key=sort_timestamp, reverse=True)
        #elif self.queue_order == self.QUEUE_ORDER_ADAPTIVE:
            #self.event_queue.sort(key=sort_timestamp)
            #self.event_queue.sort(key=sort_reference_time, reverse=True)
        self.event_queue.sort(key=sort_rpc)

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

    def event_matches_object_version(self, event):
        """!
        @brief Return True if Event.object_version() equals Resource.object_version, False otherwise.
        """
        if not isinstance(event, eva.event.ProductstatusResourceEvent):
            return False
        return event.object_version() == event.resource.object_version

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
            instances = self.productstatus.datainstance.objects.filter(data__productinstance=product_instance).order_by('created')
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
        resource = self.productstatus.datainstance[data_instance_uuid]
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
