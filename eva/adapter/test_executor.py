import eva.base.adapter
import eva.job
import eva.exceptions


class TestExecutorAdapter(eva.base.adapter.BaseAdapter):
    """
    A test adapter that echoes the URL of the received DataInstance, and then
    sleeps for 10 seconds.
    """

    def create_job(self, job):
        """!
        @brief Create a Job that echoes the URI of the received resource.
        """
        job.command = [
            "echo %s" % job.resource.url,
            "sleep 10",
        ]

    def finish_job(self, job):
        if not job.complete():
            raise eva.exceptions.RetryException('Job did not finish successfully.')
        job.logger.info('Job has finished.')

    def generate_resources(self, job, resources):
        pass
