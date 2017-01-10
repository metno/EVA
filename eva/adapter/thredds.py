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
       thredds_poll_interval        |int|           20              optional    How often the THREDDS server should be checked.
       thredds_poll_retries         |int|           6               optional    How many times to retry locating the file at the THREDDS server.
       ===========================  ==============  ==============  ==========  ===========
    """

    CONFIG = {
        'thredds_poll_interval': {
            'type': 'int',
            'help': '',
            'default': '20',
        },
        'thredds_poll_retries': {
            'type': 'int',
            'help': 'Number of times to check for the data on Thredds server.',
            'default': '6',
        },
        'thredds_base_url': {
            'type': 'string',
            'help': '',
            'default': '',
        },
    }

    REQUIRED_CONFIG = [
        'input_product',
        'input_service_backend',
        'output_service_backend',
        'thredds_base_url',
    ]

    OPTIONAL_CONFIG = [
        'input_data_format',
        'thredds_poll_interval',
        'thredds_poll_retries',
    ]

    def adapter_init(self):
        """!
        @brief Populate internal variables.
        """
        self.thredds_poll_interval = self.env['thredds_poll_interval']
        self.thredds_poll_retries = self.env['thredds_poll_retries']
        self.thredds_base_url = self.env['thredds_base_url']

    def create_job(self, job):
        """!
        @brief Check if the resource is reachable via the provided URL if not, sleep and try again
        """
        # Assuming that when the .html link is accessible so will be the dataset via OPeNDAP
        basename = os.path.basename(job.resource.url)
        job.thredds_url = os.path.join(self.thredds_base_url, basename)
        job.thredds_html_url = job.thredds_url + ".html"

        job.command = """
#!/bin/bash
#$ -S /bin/bash
for try in `seq 1 %(num_tries)d`; do
    echo "[${try}/%(num_tries)d] Try to fetch %(url)s..."
    wget --quiet --output-document=/dev/null %(url)s
    if [ $? -eq 0 ]; then
        exit 0
    fi
    if [ "$try" == "%(num_tries)d" ]; then
        echo "Document is not available; giving up."
        exit 1
    fi
    echo "Document is not available; sleeping %(sleep)d seconds..."
    sleep %(sleep)d
done
exit 1
"""
        job.command = job.command % {
            'num_tries': self.thredds_poll_retries + 1,  # correct usage of the word 'retry'
            'url': job.thredds_html_url,
            'sleep': self.thredds_poll_interval,
        }

    def finish_job(self, job):
        """!
        @brief Ignore errors but log them.
        """
        if not job.complete():
            self.logger.error('THREDDS document could not be found; ignoring error condition.')

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
