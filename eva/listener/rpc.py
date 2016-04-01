import re
import datetime
import dateutil.tz

import productstatus.event
import productstatus.exceptions

import eva.event
import eva.base.listener
import eva.exceptions


class RPCListener(eva.base.listener.BaseListener):
    """!
    @brief Listen for RPC events from Kafka.
    """

    def setup_listener(self):
        """!
        @brief Instantiate listener objects. Avoid doing this in the constructor.
        """
        self.rpc_configuration = self.kwargs['productstatus_api'].get_event_listener_configuration()
        self.event_listener = productstatus.event.Listener(
            'eva.rpc',
            bootstrap_servers=self.rpc_configuration.brokers,
            client_id=self.kwargs['client_id'],
            group_id=self.kwargs['group_id'],
            consumer_timeout_ms=1000,
        )
        self.logger.info('Instance ID for RPC calls: %s', self.kwargs['group_id'])

    def get_next_event(self):
        """!
        @brief Block until a message is received, or a timeout is reached, and
        return the message object. Raises an exception if a timeout is reached
        or the message is not indended for us.
        @returns a Message object.
        """
        try:
            event = self.event_listener.get_next_event()
            self.logger.info('Remote procedure call message received: %s', event)
            self.event_listener.save_position()
            self.filter_event(event)
            rpc = eva.rpc.RPC(event)
        except productstatus.exceptions.EventTimeoutException, e:
            raise eva.exceptions.EventTimeoutException(e)
        return eva.event.RPCEvent(
            rpc,
            timestamp=datetime.datetime.now().replace(tzinfo=dateutil.tz.tzutc())
        )

    def filter_event(self, event):
        """!
        @brief Throws an exception if an event is not intended for us, or an
        invalid regular expression is encountered.
        """
        try:
            if not re.match(event.instance_id, self.kwargs['group_id']):
                raise eva.exceptions.RPCWrongInstanceIDException(
                    'RPC call instance ID regular expression "%s" does not match my ID "%s"' % (
                        event.instance_id,
                        self.kwargs['group_id'],
                    )
                )
        except re.error, e:
            raise eva.exceptions.RPCInvalidRegexException("Invalid regular expression in event instance_id: %s" % unicode(e))
