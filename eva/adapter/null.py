import eva.job
import eva.base.adapter


class NullAdapter(eva.base.adapter.BaseAdapter):
    """!
    An adapter that matches nothing and does nothing.
    """

    def create_job(self, message_id, resource):
        job = eva.job.Job(message_id, self.globe)
        job.command = "#!/bin/sh\n/bin/true\n"
        return job

    def finish_job(self, job):
        job.logger.info('NullAdapter has successfully sent the resource to /dev/null')

    def generate_resources(self, job, resources):
        pass
