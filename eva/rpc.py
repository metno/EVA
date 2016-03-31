import dateutil.parser

import eva.exceptions


class RPC(object):
    """!
    @brief Remote Procedure Call class.

    Instantiate this class with a JSON hash containing the 'function', 'args',
    and 'kwargs' keys, then run it by using __call__. Example below:

        instance = RPC(...)
        instance()
    """

    def __init__(self, rpc_message, eventloop):
        self.eventloop = eventloop
        self.rpc_message = rpc_message
        self.function = None
        self.args = None
        self.kwargs = None
        self._parse()

    def _parse(self):
        """!
        @brief Parse the input components of the RPC request, and raise an
        Exception if any of the parameters are missing or invalid.
        """
        try:
            self.function = getattr(self, self.rpc_message.function)
            self.args = list(self.rpc_message.args)
            self.kwargs = dict(self.rpc_message.kwargs)
        except Exception, e:
            raise eva.exceptions.InvalidRPCException('Invalid RPC request: %s' % e)

    def __call__(self):
        """!
        @brief Run the RPC request.
        """
        try:
            return self.function(*self.args, **self.kwargs)
        except Exception, e:
            raise eva.exceptions.RPCFailedException(e)

    def __repr__(self):
        """!
        @brief Return a string representation of the RPC request.
        """
        return 'RPC(function=%s, args=%s, kwargs=%s)' % (self.function.__name__, self.args, self.kwargs)

    def set_message_timestamp_threshold(self, timestamp_string):
        """!
        @see Eventloop.set_message_timestamp_threshold
        """
        ts = dateutil.parser.parse(timestamp_string)
        return self.eventloop.set_message_timestamp_threshold(ts)

    def process_all_in_product_instance(self, uuid):
        """!
        @see Eventloop.set_message_timestamp_threshold
        """
        return self.eventloop.process_all_in_product_instance(uuid)

    def shutdown(self):
        """!
        @see Eventloop.shutdown
        """
        return self.eventloop.shutdown()

    def ping(self):
        """!
        @brief Null function, allowing the RPC sender to discover if the
        message is received by looking in the logs. Will not trigger any
        errors, so that log statistics looks clean.
        """
        pass
