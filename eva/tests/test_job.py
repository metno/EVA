import eva
import eva.job
import eva.mail
import eva.globe
import eva.statsd
import eva.tests


class TestJob(eva.tests.TestBase):
    def setUp(self):
        super().setUp()
        self.job = eva.job.Job('id', self.globe)

    def test_create_job_initialized(self):
        self.assertTrue(self.job.initialized())

    def test_job_ready(self):
        self.job.set_status(eva.job.READY)
        self.assertTrue(self.job.ready())

    def test_job_started(self):
        self.job.set_status(eva.job.STARTED)
        self.assertTrue(self.job.started())

    def test_job_running(self):
        self.job.set_status(eva.job.RUNNING)
        self.assertTrue(self.job.running())

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
