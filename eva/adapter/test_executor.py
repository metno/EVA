import eva.base.adapter
import eva.job


class TestExecutorAdapter(eva.base.adapter.BaseAdapter):
    """
    An adapter that echoes the URL of the received DataInstance.
    """

    def process_resource(self, resource):
        """
        @brief Execute a Job that echoes the URI of the received resource.
        """
        job = eva.job.Job()
        job.command = """#!/bin/bash
        echo %(url)s
        """ % {
            'url': resource.url,
        }
        self.execute(job)
