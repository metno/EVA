from unittest import mock

import eva.event
import eva.eventqueue
import eva.tests


class TestEventBase(eva.tests.TestBase):
    """!
    @brief Base class for EventQueue and EventQueueItem tests.
    """

    def make_event(self, data=''):
        return eva.event.Event(data, data)

    def make_events(self, count=1):
        return [self.make_event(data=str(x)) for x in range(count)]

    def make_job(self, id='foo'):
        return eva.job.Job(id, self.globe)

    def make_jobs(self, count=1):
        return [self.make_job(str(x)) for x in range(count)]


class TestEventQueue(TestEventBase):
    def setUp(self):
        super().setUp()
        self.event_queue = eva.eventqueue.EventQueue()
        self.event_queue.set_globe(self.globe)
        self.event_queue.init()

    def test_add_event(self):
        """!
        @brief Test that an event can be added to the event queue, and that the
        object is added to the items dictionary.
        """
        event = self.make_event()
        self.event_queue.add_event(event)
        self.assertEqual(len(self.event_queue), 1)
        self.assertTrue(event.id() in self.event_queue.items)
        self.assertIsInstance(self.event_queue.items[event.id()], eva.eventqueue.EventQueueItem)

    def test_add_event_duplicate_event(self):
        """!
        @brief Test that adding an event already in the queue raises an exception.
        """
        event = self.make_event()
        self.event_queue.add_event(event)
        with self.assertRaises(eva.exceptions.DuplicateEventException):
            self.event_queue.add_event(event)

    def test_add_event_non_object(self):
        """!
        @brief Test that a variable not of type eva.event.Event cannot be added
        to the event queue.
        """
        event = 'foo'
        with self.assertRaises(AssertionError):
            self.event_queue.add_event(event)

    def test_multi_event(self):
        """!
        @brief Test that multiple events can be added to the event queue, and
        that the order is correct when iterating the queue.
        """
        events = self.make_events(10)
        for event in events:
            self.event_queue.add_event(event)
        self.assertEqual(len(self.event_queue), 10)
        for index, item in enumerate(self.event_queue):
            self.assertEqual(events[index].id(), item.id())
            self.assertEqual(item.event.data, str(index))

    def test_event_keys(self):
        """!
        @brief Test that an ordered list of event keys are returned with item_keys().
        """
        events = self.make_events(3)
        keys = [event.id() for event in events]
        for event in events:
            self.event_queue.add_event(event)
        event_queue_keys = self.event_queue.item_keys()
        self.assertListEqual(keys, event_queue_keys)

    def test_event_keys_ephemeral(self):
        """!
        @brief Test that ephemeral events are not returned an ordered list of event keys are returned with item_keys().
        """
        events = self.make_events(2)
        keys = [event.id() for event in events]
        for event in events:
            self.event_queue.add_event(event)
        self.event_queue.add_event(eva.event.ProductstatusLocalEvent({}, 'http://foo/bar'))
        event_queue_keys = self.event_queue.item_keys()
        self.assertListEqual(keys, event_queue_keys)

    def test_adapter_active_job_count(self):
        """!
        @brief Test that the event queue returns the number of active jobs for
        a specific adapter.
        """
        adapter = eva.base.adapter.BaseAdapter()
        self.assertEqual(self.event_queue.adapter_active_job_count(adapter), 0)
        events = self.make_events(10)
        for event in events:
            item = self.event_queue.add_event(event)
            jobs = self.make_jobs(10)
            for job in jobs:
                job.adapter = adapter
                item.add_job(job)
            for i in range(3, 4):
                jobs[i].set_status(eva.job.STARTED)
            for i in range(4, 6):
                jobs[i].set_status(eva.job.RUNNING)
            for i in range(6, 9):
                jobs[i].set_status(eva.job.COMPLETE)
        self.assertEqual(self.event_queue.adapter_active_job_count(adapter), 30)


class TestEventQueueItem(TestEventBase):
    def setUp(self):
        super().setUp()
        self.event = eva.event.Event('foo-bar', 'foo-bar')
        self.item = eva.eventqueue.EventQueueItem(self.event)

    def test_add_job(self):
        job = self.make_job()
        self.item.add_job(job)
        self.assertEqual(len(self.item), 1)
        self.assertTrue(job.id in self.item.jobs)

    def test_add_job_non_object(self):
        job = 'foo'
        with self.assertRaises(AssertionError):
            self.item.add_job(job)

    def test_multi_job(self):
        jobs = self.make_jobs(10)
        for job in jobs:
            self.item.add_job(job)
        self.assertEqual(len(self.item), 10)
        for index, job in enumerate(self.item):
            self.assertEqual(jobs[index].id, job.id)

    def test_finished(self):
        jobs = self.make_jobs(len(eva.job.ALL_STATUSES))
        for job in jobs:
            self.item.add_job(job)
        for index, status in enumerate(eva.job.ALL_STATUSES):
            jobs[index].set_status(status)
        self.assertFalse(self.item.finished())
        for job in jobs:
            job.set_status(eva.job.FINISHED)
        self.assertTrue(self.item.finished())

    def test_serialize(self):
        jobs_serialized = {}
        jobs = self.make_jobs(2)
        for job in jobs:
            job.adapter = mock.MagicMock()
            job.adapter.config_id = 'foo'
            self.item.add_job(job)
            jobs_serialized[job.id] = {
                'adapter': job.adapter.config_id,
                'failures': job.failures(),
                'pid': job.pid,
                'status': job.status,
            }
        keys = [job.id for job in jobs]
        check = {
            'message': 'foo-bar',
            'job_keys': keys,
            'jobs': jobs_serialized,
        }
        serialized = self.item.serialize()
        self.assertDictEqual(serialized, check)
