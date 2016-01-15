import eva.job


class BaseExecutor(object):
    """
    Abstract base class for execution engines.
    """

    def __init__(self, environment_variables):
        self.env = environment_variables

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


class NullExecutor(BaseExecutor):
    """
    Pretend to execute tasks, but don't actually do it.
    """

    def execute_async(self, job):
        job.status = eva.job.STARTED

    def update_status(self, job):
        job.status = eva.job.COMPLETE
