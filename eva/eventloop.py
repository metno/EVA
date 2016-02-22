import logging

import eva


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

    def iteration(self):
        """
        @brief A single main loop iteration.
        """
        logging.debug('Waiting for next Productstatus event...')
        event = self.event_listener.get_next_event()
        logging.info('Received Productstatus event for resource URI %s' % event.uri)
        resource = self.productstatus_api[event.uri]
        logging.debug('Start processing event.')
        self.adapter.process_resource(resource)
        logging.debug('Finished processing event.')

    def run_forever(self, func, *args, **kwargs):
        """
        @brief Run a function forever, catching any RetryException that may happen.
        """
        return eva.retry_n(func,
                           args=args,
                           kwargs=kwargs,
                           exceptions=(eva.exceptions.RetryException,),
                           give_up=0)

    def __call__(self):
        """
        @brief Main loop. Checks for Productstatus events and dispatchs them to the adapter.
        """
        logging.info('Ready to start processing events.')
        while True:
            self.run_forever(self.iteration)

    def process_all_in_product_instance(self, product_instance):
        """
        @brief Process all child DataInstance objects of a ProductInstance.
        """
        instances = self.productstatus_api.datainstance.objects.filter(data__productinstance=product_instance).order_by('created')
        index = 1
        count = instances.count()
        logging.debug('Processing %d DataInstance resources that are children of ProductInstance %s', count, product_instance)
        for resource in instances:
            logging.debug('[%d/%d] Start processing resource: %s', index, count, resource)
            self.run_forever(self.adapter.process_resource, resource)
            index += 1
