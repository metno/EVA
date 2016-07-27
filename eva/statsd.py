"""
Multiple-endpoint StatsD client with Telegraf tag support.
"""


import socket
import timeit
import math


class StatsDClient(object):
    def __init__(self, tags={}, dsn_list=[]):
        self.connections = []
        self.tags = tags
        for dsn in dsn_list:
            host, port = [x.strip() for x in dsn.split(':')]
            connection = {
                'host': host,
                'port': int(port),
                'socket': socket.socket(socket.AF_INET, socket.SOCK_DGRAM),
            }
            self.connections += [connection]

    def flatten_tags(self, tags):
        """!
        @brief Reduce a dictionary of tags into a key=value list.
        """
        tag_list = []
        keys = sorted(tags.keys())
        for key in keys:
            tag_list += ['%s=%s' % (key, tags[key])]
        return ','.join(tag_list)

    def merge_tags(self, tags):
        """!
        @brief Create a complete list of key=value tags, including global tags
        set during class instantiation.
        """
        return ','.join([self.flatten_tags(self.tags), self.flatten_tags(tags)]).strip(',')

    def broadcast(self, message):
        """!
        @brief Send a message using UDP to all configured endpoints.
        """
        for connection in self.connections:
            connection['socket'].sendto(message.encode('ascii'), (connection['host'], connection['port']))

    def appended_tags(self, tags):
        """!
        @brief Create a tag string that can be directly appended to a metric message.
        """
        t = self.merge_tags(tags)
        if len(t) > 0:
            return ',' + t
        return t

    def generate_message(self, metric, value, identifier, tags):
        """!
        @brief Generate a StatsD formatted message.
        """
        return '%s%s:%d|%s' % (metric, self.appended_tags(tags), value, identifier)

    def incr(self, metric, value=1, tags={}):
        """!
        @brief Increment a metric counter.
        """
        message = self.generate_message(metric, value, 'c', tags)
        self.broadcast(message)

    def gauge(self, metric, value, tags={}):
        """!
        @brief Set a gauge metric.
        """
        message = self.generate_message(metric, value, 'g', tags)
        self.broadcast(message)

    def set(self, metric, value, tags={}):
        """!
        @brief Add to a set metric.
        """
        message = self.generate_message(metric, value, 's', tags)
        self.broadcast(message)

    def histogram(self, metric, value, tags={}):
        """!
        @brief Add a histogram metric.
        """
        message = self.generate_message(metric, value, 'h', tags)
        self.broadcast(message)

    def timing(self, metric, value, tags={}):
        """!
        @brief Add a timing metric.
        """
        message = self.generate_message(metric, value, 'ms', tags)
        self.broadcast(message)

    def timer(self, metric, tags={}):
        return StatsDTimer(self, metric, tags)



class StatsDTimer(object):
    def __init__(self, parent, metric, tags):
        self.start_time = None
        self.stop_time = None
        self.parent = parent
        self.metric = metric
        self.tags = tags

    def start(self):
        if self.start_time is not None:
            raise RuntimeError('Timer has already been started.')
        self.start_time = timeit.default_timer()
        return self

    def stop(self):
        if self.stop_time is not None:
            raise RuntimeError('Timer has already been stopped.')
        self.stop_time = timeit.default_timer()
        self.send()

    def send(self):
        if self.start_time is None or self.stop_time is None:
            raise RuntimeError('Timer has not completed successfully.')
        ms = int(math.ceil((self.stop_time - self.start_time) * 1000))
        return self.parent.timing(self.metric, ms, self.tags)
