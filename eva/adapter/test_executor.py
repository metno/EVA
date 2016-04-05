import eva.base.adapter
import eva.job


class TestExecutorAdapter(eva.base.adapter.BaseAdapter):
    """!
    An adapter that echoes the URL of the received DataInstance.
    """

    def process_resource(self, message_id, resource):
        """!
        @brief Execute a Job that echoes the URI of the received resource.
        """
        job = eva.job.Job(message_id, self.logger)
        job.command = """#!/bin/bash
        echo %(url)s
        """ % {
            'url': resource.url,
        }
        self.execute(job)
