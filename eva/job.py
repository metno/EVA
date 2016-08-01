import datetime

import eva
import eva.logger


INITIALIZED = "INITIALIZED"
STARTED = "STARTED"
COMPLETE = "COMPLETE"
FAILED = "FAILED"


class Job(object):
    """!
    The Job object holds information about which commands to execute, the job's
    state, exit status, standard output, and standard error.
    """

    def __init__(self, id, logger):
        self.id = id
        self.logger = self.create_logger(logger)
        self.command = ""  # a multi-line string containing the commands to be run
        self.exit_code = None  # process exit code
        self.stdout = []  # multi-line standard output
        self.stderr = []  # multi-line standard error
        self.set_status(INITIALIZED)  # what state the job is in
        self.next_poll_time = datetime.datetime.now()

    def set_status(self, status):
        """!
        @brief Verify and set a new Job.status variable, and log the event.
        """
        assert status in [INITIALIZED, STARTED, COMPLETE, FAILED]
        self.status = status
        self.logger.info('Setting job status to %s', self.status)

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

    def initialized(self):
        """!
        @brief Returns True if Job.status equals Job.INITIALIZED.
        """
        return self.status == INITIALIZED

    def started(self):
        """!
        @brief Returns True if Job.status equals Job.STARTED.
        """
        return self.status == STARTED

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

    def create_logger(self, logger):
        """!
        @brief Returns a custom log adapter for logging contextual information
        about jobs.
        """
        return eva.logger.JobLogAdapter(logger, {'JOB': self})
