import datetime
import dateutil.tz
import copy
import time

import eva
import eva.rpc

import productstatus.exceptions


class Eventloop(object):
    """!
    The main loop.
    """

    RECOVERABLE_EXCEPTIONS = (eva.exceptions.RetryException, productstatus.exceptions.ServiceUnavailableException,)

    def __init__(self,
                 productstatus_api,
                 event_listener,
                 rpc_event_listener,
                 adapter,
                 environment_variables,
                 logger,
                 ):
        self.event_listener = event_listener
        self.rpc_event_listener = rpc_event_listener
        self.productstatus_api = productstatus_api
        self.adapter = adapter
        self.env = environment_variables
        self.logger = logger
        self.event_queue = []
        self.rpc_queue = []
        self.message_timestamp_threshold = datetime.datetime.fromtimestamp(0, dateutil.tz.tzutc())

    def poll_productstatus(self):
        """!
        @brief Poll for Productstatus messages.
        """
        try:
            event = self.event_listener.get_next_event()
            self.event_queue += [event]
            self.logger.info('Productstatus message received: %s', event)
        except productstatus.exceptions.EventTimeoutException:
            pass

    def poll_rpc(self):
        """!
        @brief Poll for RPC messages.
        """
        try:
            event = self.rpc_event_listener.get_next_event()
            self.logger.info('Remote procedure call message received: %s', event)
            rpc = eva.rpc.RPC(event, self)
            self.logger.info("Adding RPC request to queue: %s", rpc)
            self.rpc_queue += [rpc]
        except eva.exceptions.RPCTimeoutException:
            pass
        except eva.exceptions.RPCWrongInstanceIDException, e:
            self.logger.info(e)
        except eva.exceptions.RPCInvalidRegexException:
            self.logger.warning('Invalid regular expression in event instance_id, discarding.')
        except eva.exceptions.RPCException, e:
            self.logger.error('Error when parsing RPC request, discarding: %s', e)

    def __call__(self):
        """!
        @brief Main loop. Checks for Productstatus events and dispatchs them to the adapter.
        """
        self.logger.info('Start processing events and RPC calls.')
        while True:
            # Poll for new messages
            self.poll_productstatus()
            self.poll_rpc()
            # Process RPC requests
            self.process_latest_rpc()
            # Workaround asynchronicity in database transaction; will result in fewer 404 errors
            time.sleep(0.5)
            # Process the event
            self.process_latest_resource()

    def process_latest_resource(self):
        """!
        @brief Process the latest resource message in the resource queue.
        """
        if len(self.event_queue) == 0:
            return
        try:
            event = self.event_queue[0]
            self.logger.info('Processing Productstatus event for resource URI %s' % event.uri)
            resource = self.productstatus_api[event.uri]
            # Discard message if below timestamp threshold
            if resource.modified < self.message_timestamp_threshold:
                self.logger.warning('Skip processing event because resource is older than threshold: %s vs %s',
                                    resource.modified,
                                    self.message_timestamp_threshold)
            else:
                self.adapter.validate_and_process_resource(resource)
            # Store our current message offset remotely
            self.event_listener.save_position()
            del self.event_queue[0]
        except self.RECOVERABLE_EXCEPTIONS, e:
            self.logger.error('Restarting processing of resource URI %s due to error: %s', event.uri, e)
            time.sleep(2.0)

    def process_latest_rpc(self):
        """!
        @brief Process the latest RPC message in the RPC queue.
        """
        if len(self.rpc_queue) == 0:
            return
        rpc = self.rpc_queue[0]
        self.logger.info('Processing RPC request: %s', unicode(rpc))
        try:
            rpc()
        except eva.exceptions.RPCFailedException, e:
            self.logger.error('Error while executing RPC request: %s', e)
        self.logger.info('Finished processing RPC request: %s', unicode(rpc))
        del self.rpc_queue[0]

    def set_message_timestamp_threshold(self, timestamp):
        """!
        @brief Fast-forward the message queue to a specific time.
        """
        self.message_timestamp_threshold = copy.copy(timestamp)
        self.logger.info('Forwarding message queue threshold timestamp to %s', self.message_timestamp_threshold)

    def process_all_in_product_instance(self, product_instance):
        """!
        @brief Process all child DataInstance objects of a ProductInstance.
        """
        while True:
            try:
                self.logger.info('Fetching DataInstance resources descended from %s', product_instance)
                instances = self.productstatus_api.datainstance.objects.filter(data__productinstance=product_instance).order_by('created')
                index = 1
                count = instances.count()
                self.logger.info('Processing %d DataInstance resources...', count)
                for resource in instances:
                    self.logger.info('[%d/%d] Processing %s', index, count, resource)
                    eva.retry_n(lambda: self.adapter.validate_and_process_resource(resource),
                                exceptions=self.RECOVERABLE_EXCEPTIONS,
                                give_up=0)
                    index += 1
            except self.RECOVERABLE_EXCEPTIONS, e:
                self.logger.error('A recoverable error occurred: %s', e)
                time.sleep(2.0)
                continue
            break
