import unittest
import logging
import datetime
from unittest import mock

import eva
import eva.adapter
import eva.event
import eva.eventloop
import eva.eventqueue
import eva.executor
import eva.mail
import eva.tests


class TestEventloop(eva.tests.TestBase):

    def setUp(self):
        super().setUp()
        self.adapters = []
        self.listeners = []
        self.health_check_server = None
        self.eventloop = eva.eventloop.Eventloop(self.adapters,
                                                 self.listeners,
                                                 self.health_check_server,
                                                 )
        self.eventloop.set_globe(self.globe)
        self.eventloop.init()

    def test_init(self):
        """!
        @brief Test that the Eventloop is properly initialized.
        """
        self.assertEqual(self.eventloop.adapters, self.adapters)
        self.assertEqual(self.eventloop.listeners, self.listeners)
        self.assertEqual(self.eventloop.health_check_server, self.health_check_server)
        self.assertIsInstance(self.eventloop.event_queue, eva.eventqueue.EventQueue)
        self.assertIsInstance(self.eventloop.message_timestamp_threshold, datetime.datetime)

    @unittest.skip
    def test_add_event_to_queue(self):
        event = eva.event.Event(None, {})
        self.eventloop.add_event_to_queue(event)
        self.assertListEqual(self.eventloop.event_queue, [event])

    @unittest.skip
    def test_add_event_to_queue_assert(self):
        with self.assertRaises(AssertionError):
            self.eventloop.add_event_to_queue(1)

    @unittest.skip
    def test_fill_process_list(self):
        self.eventloop.concurrency = 4
        self.eventloop.event_queue = [eva.event.ProductstatusLocalEvent(None, {}, timestamp=eva.now_with_timezone()) for x in range(5)]
        self.eventloop.fill_process_list()
        self.assertEqual(len(self.eventloop.process_list), 4)
        self.assertEqual(len(self.eventloop.event_queue), 1)

    @unittest.skip
    def test_remove_event_from_queues(self):
        self.eventloop.process_list = [1, 2, 3]
        self.eventloop.event_queue = [1, 2, 4]
        self.eventloop.remove_event_from_queues(2)
        self.assertListEqual(self.eventloop.process_list, [1, 3])
        self.assertListEqual(self.eventloop.event_queue, [1, 4])

    @unittest.skip
    def test_process_list_full_true(self):
        self.eventloop.concurrency = 4
        self.eventloop.process_list = ['a', 'b', 'c', 'd']
        self.assertTrue(self.eventloop.process_list_full())

    @unittest.skip
    def test_process_list_full(self):
        self.eventloop.concurrency = 4
        self.eventloop.process_list = ['a', 'b']
        self.assertFalse(self.eventloop.process_list_full())

    @unittest.skip
    def test_process_list_empty_true(self):
        self.eventloop.process_list = []
        self.assertTrue(self.eventloop.process_list_empty())

    @unittest.skip
    def test_process_list_empty(self):
        self.eventloop.process_list = ['a']
        self.assertFalse(self.eventloop.process_list_empty())

    @unittest.skip
    def test_event_queue_empty_true(self):
        self.eventloop.event_queue = []
        self.assertTrue(self.eventloop.event_queue_empty())

    @unittest.skip
    def test_event_queue_empty(self):
        self.eventloop.event_queue = ['a']
        self.assertFalse(self.eventloop.event_queue_empty())

    @unittest.skip
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

    @unittest.skip
    def test_drained(self):
        self.eventloop.process_list = []
        self.eventloop.event_queue = ['a']
        self.assertFalse(self.eventloop.drained())
        self.eventloop.drain = True
        self.assertFalse(self.eventloop.drained())
        self.eventloop.event_queue = []
        self.assertTrue(self.eventloop.drained())

    @unittest.skip
    def test_draining(self):
        self.eventloop.drain = True
        self.assertTrue(self.eventloop.draining())
        self.eventloop.drain = False
        self.assertFalse(self.eventloop.draining())

    @unittest.skip
    def test_sort_queue(self):
        """!
        @brief Test that the event queue is sorted according to specs.

        * RPC events go first in queue
        * Events are sorted by their timestamps in chronological or reverse
          chronological order, according to EVA_QUEUE_ORDER being either FIFO
          or LIFO
        """
        timestamp = eva.now_with_timezone()
        future = timestamp + datetime.timedelta(seconds=26)
        self.eventloop.event_queue = [
            eva.event.ProductstatusLocalEvent(None, 1, timestamp=future),
            eva.event.ProductstatusLocalEvent(None, 2, timestamp=timestamp),
            eva.event.RPCEvent(None, 3, timestamp=timestamp),
            eva.event.ProductstatusLocalEvent(None, 4, timestamp=timestamp),
            eva.event.RPCEvent(None, 5, timestamp=future),
            eva.event.ProductstatusLocalEvent(None, 6, timestamp=future),
            eva.event.ProductstatusLocalEvent(None, 7, timestamp=timestamp),
            eva.event.RPCEvent(None, 8, timestamp=timestamp),
        ]
        self.eventloop.sort_queue()
        order = [3, 8, 5, 2, 4, 7, 1, 6, ]
        self.assertEqual(len(self.eventloop.event_queue), len(order))
        for i, n in enumerate(order):
            self.assertEqual(self.eventloop.event_queue[i].data, n)

        self.eventloop.queue_order = self.eventloop.QUEUE_ORDER_LIFO
        self.eventloop.sort_queue()
        order = [5, 3, 8, 1, 6, 2, 4, 7, ]
        for i, n in enumerate(order):
            self.assertEqual(self.eventloop.event_queue[i].data, n)

    @unittest.skip
    def test_sort_queue_adaptive(self):
        now = eva.now_with_timezone()
        # create six mock objects
        mocks = [mock.MagicMock() for x in range(6)]
        # set type to 'datainstance' for the first four objects, and set reference time to now
        for x in [0, 1, 2, 3]:
            mocks[x]._collection._resource_name = 'datainstance'
            mocks[x].data.productinstance.reference_time = now
        # set future reference time for the last two objects datainstance objects
        for x in [2, 3]:
            mocks[x].data.productinstance.reference_time = eva.coerce_to_utc(datetime.datetime(2100, 1, 1, 12, 0, 0))
        # set event queue contents to two ProductstatusResourceEvents and
        # three ProductstatusLocalEvents, data pointing to the mock objects,
        # with increasing timestamps
        self.eventloop.event_queue = [
            eva.event.ProductstatusLocalEvent(str(x), mocks[x], timestamp=now + datetime.timedelta(hours=x + 1))
            for x in [0, 1, 4]
        ]
        self.eventloop.event_queue += [
            eva.event.ProductstatusResourceEvent(str(x), mocks[x], timestamp=now + datetime.timedelta(hours=x + 1))
            for x in [2, 3]
        ]
        # add an rpc event to the event queue
        self.eventloop.event_queue += [eva.event.RPCEvent(len(mocks) - 1, mocks[-1], timestamp=now + datetime.timedelta(hours=12))]
        # set queue sort order
        self.eventloop.queue_order = self.eventloop.QUEUE_ORDER_ADAPTIVE
        # sort queue
        logging.info('=== PRE-SORT ===')
        [logging.info("%d: ts=%s type=%s reference_time=%s", i, x.timestamp(), x.__class__.__name__, x.data.data.productinstance.reference_time) for i, x in enumerate(self.eventloop.event_queue)]
        self.eventloop.sort_queue()
        logging.info('=== POST-SORT ===')
        [logging.info("%d: ts=%s type=%s reference_time=%s", i, x.timestamp(), x.__class__.__name__, x.data.data.productinstance.reference_time) for i, x in enumerate(self.eventloop.event_queue)]
        # test expected order: 1 rpc event first, then 2 objects with future
        # reftimes, then 2 objects with "now" reference time, then 1
        # non-datainstance object
        expected_order = [5, 2, 3, 0, 1, 4]
        real_order = [int(x.message) for x in self.eventloop.event_queue]
        for i, n in enumerate(expected_order):
            self.assertEqual(self.eventloop.event_queue[i].data, mocks[n],
                             msg='Queue was sorted incorrectly. Expected %s but got %s' % (expected_order, real_order))

    @unittest.skip
    def test_parse_queue_order(self):
        for key, value in self.eventloop.QUEUE_ORDERS.items():
            self.assertEqual(self.eventloop.parse_queue_order(key.lower()), value)
        with self.assertRaises(eva.exceptions.InvalidConfigurationException):
            self.eventloop.parse_queue_order('foobar')
