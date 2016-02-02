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
        logging.info('Starting main loop.')
        while True:
            logging.info('Waiting for next Productstatus event...')
            event = self.event_listener.get_next_event()
            logging.info('Received Productstatus event for resource URI %s' % event.uri)
            resource = self.productstatus_api[event.uri]
            logging.info('Start processing event.')
            self.adapter.process_event(event, resource)
            logging.info('Finished processing event.')
