import copy
import datetime
import dateutil.tz
import kazoo.exceptions
import logging
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
        self.drain = False
        self.event_queue = eva.eventqueue.EventQueue()
        self.event_queue.set_globe(self.globe)
        self.event_queue.init()
        self.create_event_queue_timer()
        self.reset_event_queue_item_generator()
        self.do_shutdown = False
        self.message_timestamp_threshold = datetime.datetime.fromtimestamp(0, dateutil.tz.tzutc())

        self.statsd.gauge('eva_adapter_count', len(self.adapters))

        event_listener_configuration = self.productstatus.get_event_listener_configuration()
        if hasattr(event_listener_configuration, 'heartbeat_interval'):
            self.set_health_check_skip_heartbeat(False)
            self.set_health_check_heartbeat_interval(int(event_listener_configuration.heartbeat_interval))
            self.set_health_check_heartbeat_timeout(self.HEALTH_CHECK_HEARTBEAT_TIMEOUT)

    def adapter_by_config_id(self, config_id):
        """!
        @brief Given an adapter configuration ID, return an adapter object, or None if not found.
        @returns eva.base.adapter.BaseAdapter
        """
        for adapter in self.adapters:
            if adapter.config_id == config_id:
                return adapter
        return None

    def restore_queue(self):
        """!
        @brief Load a serialized, saved queue from ZooKeeper into the EventQueue instance.
        """
        self.logger.info('Restoring event queue from ZooKeeper...')
        cached_queue = self.event_queue.get_stored_queue()

        if len(cached_queue) > 0:
            self.event_queue.zk_immediate_store_disable()

            # Iterate through events
            for event_id, event_data in cached_queue.items():
                message = productstatus.event.Message(event_data['message'])
                event = eva.event.ProductstatusBaseEvent.factory(message)
                item = self.event_queue.add_event(event)
                self.statsd.incr('eva_restored_events')
                self.instantiate_productstatus_data(item.event)

                # Iterate through event-generated jobs
                for job_id, job_data in event_data['jobs'].items():
                    adapter = self.adapter_by_config_id(job_data['adapter'])
                    job = self.create_job_for_event_queue_item(item, adapter)
                    if not job:
                        self.logger.warning('Empty Job object returned, discarding saved job.')
                        continue

                    # FIXME: use better algorithm, or serialize data needed to restore to any state
                    if job_data['status'] == eva.job.COMPLETE or job_data['status'] == eva.job.FINISHED:
                        job.set_status(job_data['status'])
                    else:
                        job.set_status(eva.job.READY)

                    # Restore process ID
                    job.pid = job_data['pid']

                    item.add_job(job)
                    self.statsd.incr('eva_restored_jobs')

            self.event_queue.zk_immediate_store_enable()

        self.logger.info('Finished restoring event queue from ZooKeeper, size is %d items.', len(self.event_queue))

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
                self.logger.debug('%s: heartbeat received', event)
                listener.acknowledge()
                self.statsd.incr('eva_event_heartbeat')
                self.set_health_check_timestamp(eva.now_with_timezone())
                continue

            # Print to log
            self.logger.info('%s: event received', event)

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

    def next_event_queue_item(self):
        """!
        @brief Generator of event queue items, for sequential processing.
        @returns eva.eventqueue.EventQueueItem

        This generator is needed in order to quickly iterate the main loop. If
        a lot of events are in the event queue, it may take too much time to
        run through them all, resulting in a timeout at the Kafka queue.
        """
        for item in self.event_queue:
            yield item

    def create_event_queue_timer(self):
        """!
        @brief Create a statsd timer object that measures the event queue processing time.
        """
        self.event_queue_timer = self.statsd.timer('eva_event_queue_process_time')
        self.event_queue_timer.start()

    def reset_event_queue_item_generator(self):
        """!
        @brief Reset the event queue item generator.
        """
        self.event_queue_timer.stop()
        self.create_event_queue_timer()
        self.event_queue_item_generator = self.next_event_queue_item()

    def process_next_event(self):
        """!
        @brief Process any events in the process list once.
        """
        try:
            item = self.event_queue_item_generator.__next__()
        except StopIteration:
            self.reset_event_queue_item_generator()
            return

        # Postpone processing of event if it has a delay
        delay = item.event.get_processing_delay().total_seconds()
        if delay > 0:
            self.logger.info('Postponing processing of event queue item %s due to %.1f seconds event delay.', item, delay)
            return

        # Ask all adapters to create jobs for this event
        if item.empty():
            jobs = self.create_jobs_for_event_queue_item(item)
            if not jobs:
                self.event_queue.remove_item(item)
                return
            self.logger.info("%s: %d jobs generated, adding to queue...", item, len(jobs))
            for job in jobs:
                self.logger.info('%s: adding job %s', item, job)
                job.set_status(eva.job.READY)
                item.add_job(job)
            self.logger.info("%s: finished adding jobs to queue.", item)
            self.event_queue.store_item(item)

        # Check if any jobs for this event has failed, and recreate them if necessary
        failed_jobs = item.failed_jobs()
        if len(failed_jobs) > 0:
            self.logger.warning("%s: %d failed jobs; reloading Productstatus metadata and reinitializing jobs.", item, len(failed_jobs))
            self.instantiate_productstatus_data(item.event)

        # Reinitialize failed jobs
        for job in failed_jobs:

            job.logger.info('Reinitializing and requeueing previously failed job.')
            job.resource = item.event.resource

            if not job.adapter.validate_resource(job.resource):
                job.logger.warning('Adapter did not revalidate reloaded resource %s, discarding!', job.resource)
                item.remove_job(job.id)

            job.adapter.create_job(job)
            job.set_status(eva.job.READY)
            job.timer = self.statsd.timer('eva_execution_time')
            self.statsd.incr('eva_requeued_jobs')

        # Process RPC events
        if isinstance(item.event, eva.event.RPCEvent):
            item.event.data.set_executor_instance(self)
            self.process_rpc_event(item.event)
            return

        # Process all jobs generated from this event
        changed = []
        for job in item:

            try:
                # Only process N active jobs at a time
                job_active = (not job.initialized()) and (not job.ready())
                has_capacity = job.adapter.concurrency > self.event_queue.adapter_active_job_count(job.adapter)
                if job_active or has_capacity:
                    self.process_job(job)

            except self.RECOVERABLE_EXCEPTIONS as e:
                self.logger.error('Job %s suffered from a recoverable error: %s', job, e)
                job.set_status(eva.job.FAILED)
                changed += [item]

            if job.complete():
                job.set_status(eva.job.FINISHED)

            if job.status_changed():
                changed += [item]
                if not job.finished() and job.failures() == 1:
                    self.notify_job_failure(job)
                elif job.finished() and job.failures() > 0:
                    self.notify_job_success(job)

        # Store renewed statuses of changed jobs
        for item in set(changed):
            self.event_queue.store_item(item)

        # Remove event if finished
        if item.finished():
            self.logger.info('%s: removing finished event from event queue.', item)
            self.event_queue.remove_item(item)

    def process_job(self, job):
        """!
        @brief Run asynchronous processing of an event queue item.

        This function will, based on the status of the event:

        * Ask the Adapter to initialize the Job
        * Send the Job for execution to the Executor
        * Send a finish message to the Adapter
        """

        # Start job if it is not running
        if job.ready():
            job.logger.info('Ready to start; sending job to executor for asynchronous execution...')
            job.timer.start()
            job.adapter.executor.execute_async(job)
            job.logger.info('Job has been sent successfully to the executor.')

        # Check status of the job
        elif job.running():  # TODO: check for STARTED as well?
            if not job.poll_time_reached():
                return
            job.logger.debug('Polling executor for job status...')
            job.adapter.executor.sync(job)
            job.logger.debug('Finished polling executor for job status.')

        # Tell adapter that the job has finished
        elif job.complete() or job.failed():
            job.timer.stop()
            job.logger.info('Finished with total time %.1fs; sending to adapter for finishing.', job.timer.total_time_msec() / 1000.0)
            job.adapter.finish_job(job)
            try:
                job.adapter.generate_and_post_resources(job)
            except eva.exceptions.JobNotCompleteException as e:
                # ignore non-fatal errors
                job.logger.error(e)
                job.logger.warning('Job failed, but marking as complete due to adapter policy.')
                job.set_status(eva.job.COMPLETE)
            job.logger.info('Adapter has finished processing the job.')

    def main_loop_iteration(self):
        """!
        @brief A single iteration in the main loop.
        @returns True if there are more events to process, False otherwise.
        """
        timer = self.statsd.timer('eva_main_loop')
        timer.start()
        if self.drained():
            self.set_no_drain()
        if not self.draining():
            self.poll_listeners()
        self.process_health_check()
        self.process_next_event()
        self.report_event_queue_metrics()
        self.report_job_status_metrics()
        timer.stop()
        return not self.event_queue.empty()

    def __call__(self):
        """!
        @brief Main loop. Checks for Productstatus events and dispatchs them to the adapter.
        """
        self.logger.info('Entering main loop.')
        while not self.do_shutdown:
            try:
                self.main_loop_iteration()
            except kazoo.exceptions.ZookeeperError as e:
                self.logger.error('There is a problem with the ZooKeeper connection: %s', str(e))
                self.logger.error('EVA cannot continue to operate in this state, thanks for all the fish.')
                self.statsd.incr('eva_zookeeper_connection_loss')
                self.shutdown()
        self.logger.info('Exited main loop.')

    def report_job_status_metrics(self):
        """!
        @brief Report job status metrics to statsd.
        """
        status_count = self.event_queue.status_count()
        for status, count in status_count.items():
            self.statsd.gauge('eva_job_status_count', count, tags={'status': status})

    def report_event_queue_metrics(self):
        """!
        @brief Report event queue count metrics to statsd.
        """
        self.statsd.gauge('eva_event_queue_count', len(self.event_queue))

    def create_job_for_event_queue_item(self, item, adapter):
        """!
        @brief Given an EventQueueItem object, create a job based on its Event.
        @returns eva.job.Job
        """
        if not adapter.validate_resource(item.event.resource):
            return None

        id = item.event.id() + '.' + adapter.config_id
        job = eva.job.Job(id, self.globe)
        job.resource = item.event.resource

        try:
            adapter.create_job(job)
        except eva.exceptions.JobNotGenerated as e:
            self.logger.warning("%s: no jobs generated by %s: %s", item, adapter, e)
            return None

        job.timer = self.statsd.timer('eva_execution_time')
        job.adapter = adapter

        return job

    def create_jobs_for_event_queue_item(self, item):
        """!
        @brief Given an EventQueueItem object, run through all adapters and
        create jobs based on the Event.
        @returns List of Job objects
        """
        jobs = []

        self.logger.info('Start generating jobs for %s', item)

        for adapter in self.adapters:
            job = self.create_job_for_event_queue_item(item, adapter)

            if job is None:
                self.statsd.incr('eva_event_rejected', tags={'adapter': adapter.config_id})
                continue

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

    def load_serialized_data(self, path):
        """!
        @brief Load the stored event queue from ZooKeeper.
        @returns The loaded data.
        """
        if not self.zookeeper:
            return []
        data = eva.zk.load_serialized_data(self.zookeeper, path)
        return [eva.event.ProductstatusBaseEvent.factory(productstatus.event.Message(x)) for x in data if x]

    def instantiate_productstatus_data(self, event):
        """
        Make sure a ProductstatusResourceEvent has a Productstatus resource in Event.data.

        Retrieves a :class:`productstatus.api.Resource` object using the event data.

        :param eva.event.Event event: event object.
        """
        if type(event) is not eva.event.ProductstatusResourceEvent:
            return None
        self.logger.info('%s: loading resource data', event)
        event.resource = self.productstatus[event.data]
        eva.log_productstatus_resource_info(event.resource, self.logger, loglevel=logging.INFO)

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

    def notify_job_failure(self, job):
        """!
        @brief Send an email notifying about a failed job.
        """
        template_params = {
            'job_id': job.id,
            'adapter': job.adapter.config_id,
            'failures': job.failures(),
            'status': job.status,
        }
        subject = eva.mail.text.JOB_FAIL_SUBJECT % template_params
        text = eva.mail.text.JOB_FAIL_TEXT % template_params
        self.mailer.send_email(subject, text)

    def notify_job_success(self, job):
        """!
        @brief Set the number of failures for a specific event to zero, and
        send out an e-mail in case it recovered from a non-zero error count.
        """
        template_params = {
            'job_id': job.id,
            'adapter': job.adapter.config_id,
            'failures': job.failures(),
            'status': job.status,
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
                    resource.resource_uri,
                    timestamp=resource.modified,
                )]
                index += 1
        except self.RECOVERABLE_EXCEPTIONS as e:
            self.logger.error('An error occurred when retrieving Productstatus resources, aborting: %s', e)
            return
        [self.event_queue.add_event(event) for event in events]

    def process_data_instance(self, data_instance_uuid):
        """!
        @brief Process a single DataInstance resource.
        """
        resource = self.productstatus.datainstance[data_instance_uuid]
        event = eva.event.ProductstatusLocalEvent(
            {},
            resource.resource_uri,
            timestamp=resource.modified,
        )
        self.logger.info('Adding event with DataInstance %s to queue', resource)
        self.event_queue.add_event(event)

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
