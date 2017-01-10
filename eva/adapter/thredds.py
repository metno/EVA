import os.path

import eva
import eva.base.adapter
import eva.job
import eva.exceptions
import eva.template

import productstatus


class ThreddsAdapter(eva.base.adapter.BaseAdapter):
    """
    The ``ThreddsAdapter`` will check if a given data file is reachable through
    a THREDDS Data Server service, and post its metadata to Productstatus.

    The adapter will try at most ``thredds_poll_retries`` (+1) times to reach
    the file via its THREDDS url, and sleep ``thredds_poll_interval`` seconds
    between retries. If the file is not available after the last try, the
    adapter gives up and the job is NOT rescheduled.

    .. table::

       ===========================  ==============  ==============  ==========  ===========
       Variable                     Type            Default         Inclusion   Description
       ===========================  ==============  ==============  ==========  ===========
       input_product                                                required    See |input_product|.
       input_service_backend                                        required    See |input_service_backend|.
       output_service_backend                                       required    See |output_service_backend|.
       thredds_base_url             |string|        (empty)         required    Base THREDDS URL to prepend to the input resource base filename.
       ===========================  ==============  ==============  ==========  ===========
    """

    CONFIG = {
        'thredds_base_url': {'type': 'string', 'default': '', },
    }

    REQUIRED_CONFIG = [
        'input_product',
        'input_service_backend',
        'output_service_backend',
        'thredds_base_url',
    ]

    OPTIONAL_CONFIG = [
        'input_data_format',
    ]

    def create_job(self, job):
        # Assuming that when the .html link is accessible so will be the dataset via OPeNDAP
        basename = os.path.basename(job.resource.url)
        job.thredds_url = os.path.join(self.env['thredds_base_url'], basename)
        job.thredds_html_url = job.thredds_url + ".html"

        job.command = """
#!/bin/bash
#$ -S /bin/bash
set -e
wget --quiet --output-document=/dev/null %(url)s
"""
        job.command = job.command % {
            'url': job.thredds_html_url,
        }

    def finish_job(self, job):
        if not job.complete():
            raise eva.exceptions.RetryException('THREDDS document could not be found: %s' % job.thredds_html_url)

    def generate_resources(self, job, resources):
        """!
        @brief Generate a set of Productstatus resources based on job output.

        This adapter will post a new DataInstance using the same Data and
        ProductInstance as the input resource.
        """
        datainstance = productstatus.api.EvaluatedResource(
            self.api.datainstance.find_or_create_ephemeral, {
                'data': job.resource.data,
                'format': job.resource.format,
                'servicebackend': self.output_service_backend,
                'url': job.thredds_url,
            },
            extra_params={'expires': job.resource.expires}
        )
        resources['datainstance'] += [datainstance]
