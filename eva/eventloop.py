import logging


class Eventloop(object):
    """
    The main loop.
    """

    def __init__(self,
                 productstatus_api,
                 event_listener,
                 adapter,
                 environment_variables,
                 ):
        self.event_listener = event_listener
        self.productstatus_api = productstatus_api
        self.adapter = adapter
        self.env = environment_variables

    def __call__(self):
        """
        @brief Main loop. Checks for Productstatus events and dispatchs them to the adapter.
        """
        logging.debug('Starting main loop.')
        while True:
            logging.debug('Waiting for next Productstatus event...')
            event = self.event_listener.get_next_event()
            logging.info('Received Productstatus event for resource URI %s' % event.uri)
            resource = self.productstatus_api[event.uri]
            logging.debug('Start processing event.')
            self.adapter.process_resource(resource)
            logging.debug('Finished processing event.')

    def process_all_in_product_instance(self, product_instance):
        instances = self.productstatus_api.datainstance.objects.filter(data__productinstance=product_instance).order_by('created')
        index = 1
        count = instances.count()
        logging.debug('Processing %d DataInstance resources that are children of ProductInstance %s', count, product_instance)
        for resource in instances:
            logging.debug('[%d/%d] Start processing resource: %s', index, count, resource)
            self.adapter.process_resource(resource)
            index += 1
