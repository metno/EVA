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
        job.command = """#!/bin/bash
        echo %(url)s
        """ % {
            'url': resource.url,
        }
        return job

    def finish_job(self, job):
        if job.complete():
            job.logger.info('Job has finished.')
        else:
            raise eva.exceptions.RetryException('Job did not finish successfully.')
