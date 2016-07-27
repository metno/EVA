import unittest
import mock

import eva.statsd


class TestStatsD(unittest.TestCase):
    def setUp(self):
        self.tags = {
            'a': '1',
            'b': '2',
        }
        self.dsn_list = ['127.0.0.1:8125', '127.0.0.2:8126']
        self.statsd = eva.statsd.StatsDClient(self.tags, self.dsn_list)

    def test_instance(self):
        self.assertEqual(self.statsd.connections[0]['host'], '127.0.0.1')
        self.assertEqual(self.statsd.connections[0]['port'], 8125)
        self.assertEqual(self.statsd.connections[1]['host'], '127.0.0.2')
        self.assertEqual(self.statsd.connections[1]['port'], 8126)

    def test_flatten_tags(self):
        self.assertEqual(self.statsd.flatten_tags({'c': 'x'}), 'c=x')
        self.assertEqual(self.statsd.flatten_tags({'y': 'x', 'x': 'y'}), 'x=y,y=x')

    def test_merge_tags(self):
        self.assertEqual(self.statsd.merge_tags({}), 'a=1,b=2')
        self.assertEqual(self.statsd.merge_tags({'c': 'x'}), 'a=1,b=2,c=x')
        self.assertEqual(self.statsd.merge_tags({'c': 'x', 'y': 'b'}), 'a=1,b=2,c=x,y=b')

    def test_appended_tags(self):
        self.assertEqual(self.statsd.appended_tags({}), ',a=1,b=2')
        self.assertEqual(self.statsd.appended_tags({'foo': 'bar'}), ',a=1,b=2,foo=bar')
        self.statsd.tags = {}
        self.assertEqual(self.statsd.appended_tags({}), '')
        self.assertEqual(self.statsd.appended_tags({'foo': 'bar'}), ',foo=bar')

    @mock.patch('socket.socket.sendto')
    def test_broadcast(self, func):
        self.statsd.broadcast('foo')
        self.assertTrue(func.called)
        self.assertEqual(func.call_count, 2)
        func.assert_called_with(b'foo', ('127.0.0.2', 8126))

    @mock.patch('eva.statsd.StatsDClient.broadcast')
    def test_incr(self, func):
        self.statsd.incr('foo', 1)
        func.assert_called_with('foo:1,a=1,b=2|c')
