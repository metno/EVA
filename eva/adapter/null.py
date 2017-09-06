import eva.job
import eva.base.adapter


class NullAdapter(eva.base.adapter.BaseAdapter):
    """
    This adapter will generate jobs that runs ``/bin/true``. Only use this
    adapter for test purposes.
    """

    def create_job(self, job):
        job.command = ["/bin/true"]

    def finish_job(self, job):
        job.logger.info('NullAdapter has successfully sent the resource to /dev/null')

    def generate_resources(self, job, resources):
        pass
