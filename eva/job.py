class Job(object):
    """
    The Job object holds information about which commands to execute, the job's
    state, exit status, standard output, and standard error.
    """

    PREPARED = "PREPARED"
    STARTED = "STARTED"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"

    def __init__(self):
        self.command = ""  # a multi-line string containing the commands to be run
        self.status = None  # some object, set by the executor
        self.exit_code = None  # process exit code
        self.pid = None  # process id
        self.stdout = []  # multi-line standard output
        self.stderr = []  # multi-line standard error
