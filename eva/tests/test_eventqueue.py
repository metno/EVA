import eva.event
import eva.eventqueue
import eva.tests


class TestEventQueue(eva.tests.TestBase):
    def setUp(self):
        super().setUp()
        self.event_queue = eva.eventqueue.EventQueue()
        self.event_queue.set_globe(self.globe)
        self.event_queue.init()

    def make_event(self, data=''):
        return eva.event.Event(data, data)

    def test_add_event(self):
        event = self.make_event()
        self.event_queue.add_event(event)
        self.assertEqual(len(self.event_queue), 1)
        self.assertTrue(event.id() in self.event_queue.items)
        self.assertIsInstance(self.event_queue.items[event.id()], eva.eventqueue.EventQueueItem)

    def test_multi_event(self):
        events = [self.make_event(data=str(x)) for x in range(10)]
        for event in events:
            self.event_queue.add_event(event)
        self.assertEqual(len(self.event_queue), 10)
        for index, item in enumerate(self.event_queue):
            self.assertEqual(events[index].id(), item.id())
            self.assertEqual(item.event.data, str(index))


class TestEventQueueItem(eva.tests.TestBase):
    def setUp(self):
        super().setUp()
        self.event = eva.event.Event('', '')
        self.item = eva.eventqueue.EventQueueItem(self.event)

    def test_add_job(self):
        job = eva.job.Job('foo', self.globe)
        self.item.add_job(job)
        self.assertEqual(len(self.item), 1)
        self.assertTrue(job.id in self.item.jobs)

    def test_multi_job(self):
        jobs = [eva.job.Job(str(x), self.globe) for x in range(10)]
        for job in jobs:
            self.item.add_job(job)
        self.assertEqual(len(self.item), 10)
        for index, job in enumerate(self.item):
            self.assertEqual(jobs[index].id, job.id)

    def test_finished(self):
        jobs = [eva.job.Job(str(x), self.globe) for x in range(len(eva.job.ALL_STATUSES))]
        for job in jobs:
            self.item.add_job(job)
        for index, status in enumerate(eva.job.ALL_STATUSES):
            jobs[index].set_status(status)
        self.assertFalse(self.item.finished())
        for job in jobs:
            job.set_status(eva.job.FINISHED)
        self.assertTrue(self.item.finished())


