import datetime

import eva
import eva.logger
import eva.globe


INITIALIZED = "INITIALIZED"
READY = "READY"
STARTED = "STARTED"
RUNNING = "RUNNING"
COMPLETE = "COMPLETE"
FAILED = "FAILED"
DELETED = "DELETED"
FINISHED = "FINISHED"

ALL_STATUSES = (
    INITIALIZED,
    READY,
    STARTED,
    RUNNING,
    COMPLETE,
    FAILED,
    DELETED,
    FINISHED,
)


class Job(eva.globe.GlobalMixin):
    """!
    The Job object holds information about which commands to execute, the job's
    state, exit status, standard output, and standard error.
    """

    def __init__(self, id, globe):
        self.id = id
        self.globe = globe
        self.logger = self.create_logger(self.logger)
        self.adapter = None  # reference to adapter class that owns the job
        self.command = ""  # a multi-line string containing the commands to be run
        self.exit_code = None  # process exit code
        self.pid = None  # process id, executor dependent
        self.stdout = []  # multi-line standard output
        self.stderr = []  # multi-line standard error
        self.set_status(INITIALIZED)  # what state the job is in
        self.next_poll_time = eva.now_with_timezone()
        self.next_retry_time = eva.now_with_timezone()
        self.set_retry_parameters(-1, 1, 1)
        self._status_changed = False
        self._failures = 0

    def __repr__(self):
        return '<Job: %s>' % self.id

    def set_retry_parameters(self, retries, interval, backoff_factor):
        """
        Set the max retries, retry interval, and retry backoff factor variables.

        :param int retries: how many times to retry this job before satisfying :meth:`max_retries_reached`.
        :param int interval: how many seconds to wait between each retry of the job, as reported by :meth:`retry_time_reached`.
        :param int backoff_factor: each time a job fails, the retry interval is increased by this factor.
        """
        self.retry_max = retries
        self.retry_interval = interval
        self.retry_backoff_factor = backoff_factor

    def incr_retry_interval(self):
        """
        Increase the retry interval by the pre-configured backoff factor.
        """
        self.retry_interval *= self.retry_backoff_factor
        self.logger.info('Increasing retry interval with factor %.2f to %d seconds', self.retry_backoff_factor, self.retry_interval)

    def set_next_retry_time(self, secs):
        """
        Set the next time the job will be attempted.

        :param int secs: difference, in seconds, from the current timestamp.
        """
        self.next_retry_time = eva.now_with_timezone() + datetime.timedelta(seconds=secs)
        self.logger.info('Next retry for this job: %s', eva.strftime_iso8601(self.next_retry_time))

    def retry_time_reached(self):
        """
        Returns True if the job can be run, according to the retry time.
        """
        return eva.now_with_timezone() >= self.next_retry_time

    def max_retries_reached(self):
        """
        Returns True if this job has been run the maximum allowed number of times.
        """
        return self.retry_max >= 0 and self.failures() > self.retry_max

    def set_status(self, status):
        """!
        @brief Verify and set a new Job.status variable, and log the event.
        """
        assert status in ALL_STATUSES
        self.status = status
        self.logger.info('Setting job status to %s', self.status)
        if status == FAILED:
            self.incr_failures()
            self.set_next_retry_time(self.retry_interval)
            self.incr_retry_interval()
        if self.adapter:
            self.statsd.incr('eva_job_status_change', tags={
                'status': self.status,
                'adapter': self.adapter.config_id,
            })
        self._status_changed = True

    def status_changed(self):
        r = self._status_changed
        if r:
            self._status_changed = False
        return r

    def set_next_poll_time(self, msecs):
        """!
        @brief Specify how long time the Eventloop should wait before polling
        the status of this job again.
        """
        self.next_poll_time = eva.now_with_timezone() + datetime.timedelta(milliseconds=msecs)
        self.logger.debug('Next poll for this job: %s', eva.strftime_iso8601(self.next_poll_time))

    def poll_time_reached(self):
        """!
        @brief Returns True if the Job object can be polled according to
        Job.next_poll_time.
        """
        return eva.now_with_timezone() >= self.next_poll_time

    def create_logger(self, logger):
        """!
        @brief Returns a custom log adapter for logging contextual information
        about jobs.
        """
        return eva.logger.JobLogAdapter(logger, {'JOB': self})

    def set_failures(self, failures):
        """!
        @brief Set the number of processing failures for this job.
        @rtype int
        @returns The number of failures registered.
        """
        self._failures = failures
        self.statsd.incr('eva_job_failures')
        return failures

    def incr_failures(self):
        """!
        @brief Increase the number of processing failures for this job.
        @rtype int
        @returns The number of failures registered.
        """
        failures = self.failures() + 1
        return self.set_failures(failures)

    def failures(self):
        """!
        @brief Return the number of processing failures for this job.
        @rtype int
        @returns The number of failures registered.
        """
        return self._failures

    def initialized(self):
        """!
        @brief Returns True if Job.status equals Job.INITIALIZED.
        """
        return self.status == INITIALIZED

    def ready(self):
        """!
        @brief Returns True if Job.status equals Job.READY.
        """
        return self.status == READY

    def started(self):
        """!
        @brief Returns True if Job.status equals Job.STARTED.
        """
        return self.status == STARTED

    def running(self):
        """!
        @brief Returns True if Job.status equals Job.RUNNING.
        """
        return self.status == RUNNING

    def complete(self):
        """!
        @brief Returns True if Job.status equals Job.COMPLETE.
        """
        return self.status == COMPLETE

    def failed(self):
        """!
        @brief Returns True if Job.status equals Job.FAILED.
        """
        return self.status == FAILED

    def deleted(self):
        """!
        @brief Returns True if Job.status equals Job.DELETED.
        """
        return self.status == DELETED

    def finished(self):
        """!
        @brief Returns True if Job.status equals Job.FINISHED.
        """
        return self.status == FINISHED
