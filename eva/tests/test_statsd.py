import time
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

    def test_generate_message(self):
        self.assertEqual(self.statsd.generate_message('foo', 13, 'x', {}), 'foo,a=1,b=2:13|x')

    def test_timer(self):
        timer = self.statsd.timer('foo', {'bar': 'baz'})
        self.assertIsInstance(timer, eva.statsd.StatsDTimer)
        self.assertEqual(timer.parent, self.statsd)
        self.assertEqual(timer.metric, 'foo')
        self.assertDictEqual(timer.tags, {'bar': 'baz'})


    @mock.patch('socket.socket.sendto')
    def test_broadcast(self, func):
        self.statsd.broadcast('foo')
        self.assertTrue(func.called)
        self.assertEqual(func.call_count, 2)
        func.assert_called_with(b'foo', ('127.0.0.2', 8126))

    @mock.patch('eva.statsd.StatsDClient.broadcast')
    def test_incr(self, func):
        self.statsd.incr('foo', 1, {'c': 3})
        func.assert_called_with('foo,a=1,b=2,c=3:1|c')

    @mock.patch('eva.statsd.StatsDClient.broadcast')
    def test_timing(self, func):
        self.statsd.timing('foo', 200, {'c': 3})
        func.assert_called_with('foo,a=1,b=2,c=3:200|ms')



class TestStatsDTimer(unittest.TestCase):
    def setUp(self):
        self.statsd = mock.MagicMock()
        self.statsd.timing = mock.MagicMock()
        self.timer = eva.statsd.StatsDTimer(self.statsd, 'foo', {'foobar': 'baz'})

    def test_start_stop(self):
        self.timer.start()
        time.sleep(0.005)
        self.timer.stop()
        self.assertEqual(self.statsd.timing.call_count, 1)
        args, kwargs = self.statsd.timing.call_args
        self.assertEqual(args[0], 'foo')
        self.assertGreaterEqual(args[1], 5)
        self.assertDictEqual(args[2], {'foobar': 'baz'})

    def test_double_start(self):
        self.timer.start()
        with self.assertRaises(RuntimeError):
            self.timer.start()

    def test_only_stop(self):
        with self.assertRaises(RuntimeError):
            self.timer.stop()

    def test_double_stop(self):
        self.timer.start()
        self.timer.stop()
        with self.assertRaises(RuntimeError):
            self.timer.stop()
