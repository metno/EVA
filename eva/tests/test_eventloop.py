import unittest
import logging

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
        self.executor = eva.executor.NullExecutor(self.env, self.logger, self.zookeeper)
        self.adapter = eva.adapter.NullAdapter(self.env, self.executor, self.productstatus_api, self.logger, self.zookeeper)
        self.eventloop = eva.eventloop.Eventloop(self.productstatus_api,
                                                 [],
                                                 self.adapter,
                                                 self.env,
                                                 self.logger,
                                                 )

    def test_queue_empty_true(self):
        self.eventloop.event_queue = []
        self.assertTrue(self.eventloop.queue_empty())

    def test_queue_empty(self):
        self.eventloop.event_queue = ['a']
        self.assertFalse(self.eventloop.queue_empty())

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

    def test_shift_queue(self):
        self.eventloop.event_queue = [1, 2, 3]
        self.eventloop.shift_queue()
        self.assertListEqual(self.eventloop.event_queue, [2, 3])
