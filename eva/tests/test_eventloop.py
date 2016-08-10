import unittest
import logging

import eva
import eva.event
import eva.eventloop
import eva.adapter
import eva.executor

import productstatus.api


class TestEventloop(unittest.TestCase):

    def setUp(self):
        self.env = {}
        self.productstatus_api = productstatus.api.Api('http://localhost:8000')
        self.logger = logging
        self.zookeeper = None
        self.concurrency = 1
        self.statsd = eva.statsd.StatsDClient()
        self.executor = eva.executor.NullExecutor(None, self.env, self.logger, self.zookeeper, self.statsd)
        self.adapter = eva.adapter.NullAdapter(self.env, self.executor, self.productstatus_api, self.logger, self.zookeeper, self.statsd)
        self.eventloop = eva.eventloop.Eventloop(self.productstatus_api,
                                                 [],
                                                 self.adapter,
                                                 self.executor,
                                                 self.statsd,
                                                 self.zookeeper,
                                                 self.concurrency,
                                                 self.env,
                                                 self.logger,
                                                 )

    def test_add_event_to_queue(self):
        event = eva.event.Event(None, {})
        self.eventloop.add_event_to_queue(event)
        self.assertListEqual(self.eventloop.event_queue, [event])

    def test_add_event_to_queue_assert(self):
        with self.assertRaises(AssertionError):
            self.eventloop.add_event_to_queue(1)

    def test_fill_process_list(self):
        self.eventloop.concurrency = 4
        self.eventloop.event_queue = [eva.event.ProductstatusLocalEvent(None, {}, timestamp=eva.now_with_timezone()) for x in range(5)]
        self.eventloop.fill_process_list()
        self.assertEqual(len(self.eventloop.process_list), 4)
        self.assertEqual(len(self.eventloop.event_queue), 1)

    def test_remove_event_from_queues(self):
        self.eventloop.process_list = [1, 2, 3]
        self.eventloop.event_queue = [1, 2, 4]
        self.eventloop.remove_event_from_queues(2)
        self.assertListEqual(self.eventloop.process_list, [1, 3])
        self.assertListEqual(self.eventloop.event_queue, [1, 4])

    def test_process_list_full_true(self):
        self.eventloop.concurrency = 4
        self.eventloop.process_list = ['a', 'b', 'c', 'd']
        self.assertTrue(self.eventloop.process_list_full())

    def test_process_list_full(self):
        self.eventloop.concurrency = 4
        self.eventloop.process_list = ['a', 'b']
        self.assertFalse(self.eventloop.process_list_full())

    def test_process_list_empty_true(self):
        self.eventloop.process_list = []
        self.assertTrue(self.eventloop.process_list_empty())

    def test_process_list_empty(self):
        self.eventloop.process_list = ['a']
        self.assertFalse(self.eventloop.process_list_empty())

    def test_event_queue_empty_true(self):
        self.eventloop.event_queue = []
        self.assertTrue(self.eventloop.event_queue_empty())

    def test_event_queue_empty(self):
        self.eventloop.event_queue = ['a']
        self.assertFalse(self.eventloop.event_queue_empty())

    def test_both_queues_empty(self):
        self.eventloop.process_list = []
        self.eventloop.event_queue = ['a']
        self.assertFalse(self.eventloop.both_queues_empty())
        self.eventloop.process_list = ['a']
        self.eventloop.event_queue = []
        self.assertFalse(self.eventloop.both_queues_empty())
        self.eventloop.process_list = ['a']
        self.eventloop.event_queue = ['a']
        self.assertFalse(self.eventloop.both_queues_empty())
        self.eventloop.process_list = []
        self.eventloop.event_queue = []
        self.assertTrue(self.eventloop.both_queues_empty())

    def test_drained(self):
        self.eventloop.process_list = []
        self.eventloop.event_queue = ['a']
        self.assertFalse(self.eventloop.drained())
        self.eventloop.drain = True
        self.assertFalse(self.eventloop.drained())
        self.eventloop.event_queue = []
        self.assertTrue(self.eventloop.drained())

    def test_draining(self):
        self.eventloop.drain = True
        self.assertTrue(self.eventloop.draining())
        self.eventloop.drain = False
        self.assertFalse(self.eventloop.draining())

    @unittest.skip
    def test_sort_queue(self):
        self.eventloop.event_queue = [
            eva.event.ProductstatusEvent(1),
            eva.event.ProductstatusEvent(2),
            eva.event.RPCEvent(3),
            eva.event.ProductstatusEvent(4),
            eva.event.RPCEvent(5),
            eva.event.ProductstatusEvent(6),
            eva.event.ProductstatusEvent(7),
            eva.event.RPCEvent(8),
        ]
        self.eventloop.sort_queue()
        self.assertEqual(len(self.eventloop.event_queue), 8)
        self.assertEqual(self.eventloop.event_queue[0].data, 3)
        self.assertEqual(self.eventloop.event_queue[1].data, 5)
        self.assertEqual(self.eventloop.event_queue[2].data, 8)
        self.assertEqual(self.eventloop.event_queue[3].data, 1)
        self.assertEqual(self.eventloop.event_queue[4].data, 2)
        self.assertEqual(self.eventloop.event_queue[5].data, 4)
        self.assertEqual(self.eventloop.event_queue[6].data, 6)
        self.assertEqual(self.eventloop.event_queue[7].data, 7)
