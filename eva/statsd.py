"""
Multiple-endpoint StatsD client with Telegraf tag support.
"""


import socket


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

    def incr(self, metric, value=1, tags={}):
        """!
        @brief Increment a metric counter.
        """
        message = '%s:%d%s|c' % (metric, value, self.appended_tags(tags))
        self.broadcast(message)
