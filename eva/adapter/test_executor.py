import eva.base.adapter
import eva.job
import eva.exceptions


class TestExecutorAdapter(eva.base.adapter.BaseAdapter):
    """!
    An adapter that echoes the URL of the received DataInstance.
    """

    def create_job(self, message_id, resource):
        """!
        @brief Create a Job that echoes the URI of the received resource.
        """
        job = eva.job.Job(message_id, self.logger)
        job.command = """
#!/bin/bash
#$ -S /bin/bash
echo %(url)s
        """ % {
            'url': resource.url,
        }
        return job

    def finish_job(self, job):
        if not job.complete():
            raise eva.exceptions.RetryException('Job did not finish successfully.')
        job.logger.info('Job has finished.')

    def generate_resources(self, job, resources):
        pass
