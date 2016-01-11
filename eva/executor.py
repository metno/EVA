class BaseExecutor(object):
    """
    Abstract base class for execution engines.
    """

    def __init__(self):
        pass

    def execute_async(self, job):
        """
        Execute the job asynchronously, and return immediately.
        """
        raise NotImplementedError()

    def update_status(self, job):
        """
        Check job status, and update the Job object accordingly, populating
        Job.exit_code, Job.stdout, Job.stderr, and Job.status.
        """
        raise NotImplementedError()
