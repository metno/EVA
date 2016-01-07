
import datetime
import dateutil.parser
import logging
import urlparse

import productstatus.api
import productstatus.event


class DumpHandler(object):
    def __call__(self, api, message, resource):
        logging.info("got message '%s'" % message)


def traverse_resources(resource, dotted_keys):
    keys = dotted_keys.split('.')
    for key in keys:
        resource = getattr(resource, key)
    return resource


def operator_generic(actual, operator, expected):
    logging.debug("actual='%s' op='%s' expected='%s'" % (actual, operator, expected))
    if operator == 'EQ':
        return actual == expected
    elif operator == 'LT':
        return actual < expected
    elif operator == 'LE':
        return actual <= expected
    elif operator == 'GT':
        return actual > expected
    elif operator == 'GE':
        return actual >= expected
    else:
        raise Exception("Unknown operator '%s'" % operator)


def operator_datetime(actual, operator, expected):
    expected = dateutil.parser.parse(expected)
    return operator_generic(actual, operator, expected)


class FilterOperatorHandler(object):
    def __init__(self, filters, handler):
        self.filters = filters
        self.chain = handler

    def __call__(self, api, message, resource):
        if self.accept_message(api, message, resource):
            self.chain(api, message, resource)

    def accept_message(self, api, message, resource):
        for item, operator, expected in self.filters:
            logging.debug("item='%s' op='%s' exp='%s'" % (item, operator, expected))
            try:
                actual = traverse_resources(resource, item)
            except KeyError, e:
                logging.debug("no key '%s' in '%s', probably undesired type, rejecting" % (item, resource))
                logging.debug("key error is %s" % e)
                return False
            if not self.accept_filter(actual, operator, expected):
                return False
        #  all existing filters passed
        return True

    def accept_filter(self, actual, operator, expected):
        if isinstance(actual, datetime.datetime):
            return operator_datetime(actual, operator, expected)
        else:
            return operator_generic(actual, operator, expected)


class Listener(object):
    ID = u'id'
    URL = u'url'
    TYPE = u'type'
    RESOURCE = u'resource'

    def __init__(self, **kwargs):
        self.productstatus_api = productstatus.api.Api(
            kwargs['productstatus_url'],
            username=kwargs['productstatus_username'],
            api_key=kwargs['productstatus_api_key'],
            verify_ssl=kwargs['productstatus_verify_ssl'],
        )

        self.productstatus_listener = productstatus.event.Listener(kwargs['productstatus_zmq_sub'])

        self.handler = kwargs['handler']

    def listen(self):
        while True:
            message = self.productstatus_listener.get_next_event()
            self.handle_message(message)

    def handle_message(self, message):
        m_type = message[self.TYPE]
        if m_type != self.RESOURCE:
            logging.info("Can only handle of type 'resource', not '%s', ignoring this message." % m_type)
            return False

        # m_resource = message[self.RESOURCE]
        # m_id = message[self.ID]

        m_url = urlparse.urlsplit(message[self.URL])
        # server = urlparse.urlunsplit((m_url.scheme, m_url.netloc, '', '', ''))
        path = urlparse.urlunsplit(('', '', m_url.path, m_url.query, m_url.fragment))

        resource = self.productstatus_api[path]
        logging.debug("got resource: %s" % resource)

        self.handler(self.productstatus_api, message, resource)
