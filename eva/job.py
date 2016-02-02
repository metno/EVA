import uuid
import logging


INITIALIZED = "INITIALIZED"
STARTED = "STARTED"
COMPLETE = "COMPLETE"
FAILED = "FAILED"


class Job(object):
    """
    The Job object holds information about which commands to execute, the job's
    state, exit status, standard output, and standard error.
    """

    def __init__(self):
        self.id = uuid.uuid4()
        self.command = ""  # a multi-line string containing the commands to be run
        self.exit_code = None  # process exit code
        self.stdout = []  # multi-line standard output
        self.stderr = []  # multi-line standard error
        self.set_status(INITIALIZED)  # what state the job is in

    def set_status(self, status):
        assert status in [INITIALIZED, STARTED, COMPLETE, FAILED]
        self.status = status
        logging.info('[%s] Setting job status to %s' % (self.id, self.status))
