import unittest
import logging

import eva
import eva.job


class TestJob(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger('root')
        self.job = eva.job.Job('id', self.logger)

    def test_create_job_initialized(self):
        self.assertTrue(self.job.initialized())

    def test_job_started(self):
        self.job.set_status(eva.job.STARTED)
        self.assertTrue(self.job.started())

    def test_job_complete(self):
        self.job.set_status(eva.job.COMPLETE)
        self.assertTrue(self.job.complete())

    def test_job_failed(self):
        self.job.set_status(eva.job.FAILED)
        self.assertTrue(self.job.failed())

    def test_set_next_poll_time(self):
        self.job.set_next_poll_time(1000)
        self.assertGreater(self.job.next_poll_time, eva.now_with_timezone())
        self.assertFalse(self.job.poll_time_reached())

    def test_poll_time_reached(self):
        self.job.set_next_poll_time(-1)
        self.assertTrue(self.job.poll_time_reached())