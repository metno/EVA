import re

import productstatus.event
import productstatus.exceptions

import eva.exceptions


class RPCListener(productstatus.event.Listener):
    """!
    @brief Message subscriber, listening for RPC events.

    For simplicity, this class uses the same mechanism for event processing as
    the Productstatus client library.
    """

    def get_next_event(self):
        """!
        @brief Block until a message is received, or a timeout is reached, and
        return the message object. Raises an exception if a timeout is reached
        or the message is not indended for us.
        @returns a Message object.
        """
        try:
            event = super(RPCListener, self).get_next_event()
            self.save_position()
        except productstatus.exceptions.EventTimeoutException as e:
            raise eva.exceptions.RPCTimeoutException(e)
        self.filter_event(event)
        return event

    def filter_event(self, event):
        """!
        @brief Throws an exception if an event is not intended for us, or an
        invalid regular expression is encountered.
        """
        try:
            if not re.match(event.instance_id, self.group_id):
                raise eva.exceptions.RPCWrongInstanceIDException(
                    'RPC call instance ID regular expression "%s" does not match my ID "%s"' % (
                        event.instance_id,
                        self.group_id,
                    )
                )
        except re.error as e:
            raise eva.exceptions.RPCInvalidRegexException(e)
