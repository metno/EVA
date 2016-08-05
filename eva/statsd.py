"""
StatsD module, providing functions for reporting metric data to a Telegraf
StatsD implementation.
"""


import socket
import timeit
import math


class StatsDClient(object):
    """!
    @brief Multiple-endpoint StatsD client with Telegraf tag support.
    @param tags A dictionary of tags to always send with reported data.
    @param dsn_list A list of connection strings, in the form <host>:<port>.

    Use this class to send metrics to several StatsD hosts at once, for
    instance when a high-availability setup is required, and StatsD is running
    on at least one of several known host/port configurations.

    The following functions are available for metric reporting:

        incr(): increment a counter
        gauge(): set a gauge value
        set(): add to a set value
        histogram(): add to a histogram set
        timing(): report a timing value
        timer(): return a new timing object
    """

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
        return '%s%s:%d|%s\n' % (metric, self.appended_tags(tags), value, identifier)

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
        @brief Add a timing metric. Values are in milliseconds.
        """
        message = self.generate_message(metric, value, 'ms', tags)
        self.broadcast(message)

    def timer(self, metric, tags={}):
        """!
        @brief Return a new timer object.
        """
        return StatsDTimer(self, metric, tags)



class StatsDTimer(object):
    """!
    @brief StatsD timing class

    This class can be instantiated using StatsDClient.timer(). The resulting
    instance can be used to run timings in your code and automatically report
    them to StatsD. Usage is:

        timer = client.timer('metric_name')
        timer.start()
        # run some code
        timer.stop()

    Any other usage will result in a RuntimeError.
    """

    def __init__(self, parent, metric, tags):
        self.start_time = None
        self.stop_time = None
        self.parent = parent
        self.metric = metric
        self.tags = tags

    def start(self):
        """!
        @brief Start the timer.
        """
        if self.start_time is not None:
            raise RuntimeError('Timer has already been started.')
        self.start_time = timeit.default_timer()
        return self

    def stop(self):
        """!
        @brief Stop the timer, and report the data to StatsD.
        """
        if self.stop_time is not None:
            raise RuntimeError('Timer has already been stopped.')
        self.stop_time = timeit.default_timer()
        self.send()

    def total_time_msec(self):
        """!
        @brief Return the total time elapsed in milliseconds.
        """
        if self.start_time is None or self.stop_time is None:
            raise RuntimeError('Timer has not been started and stopped.')
        return int(math.ceil((self.stop_time - self.start_time) * 1000))

    def send(self):
        """!
        @brief Report the timer data to StatsD.
        """
        if self.start_time is None or self.stop_time is None:
            raise RuntimeError('Timer has not completed successfully.')
        return self.parent.timing(self.metric, self.total_time_msec(), self.tags)
