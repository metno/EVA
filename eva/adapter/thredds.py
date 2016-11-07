import os.path

import eva
import eva.base.adapter
import eva.job
import eva.exceptions
import eva.template

import productstatus


class ThreddsAdapter(eva.base.adapter.BaseAdapter):
    """!
    An adapter that listens to events about files being produced on /lustre
    and then polls the Thredds server until a matching dataset is available
    there, then it posts an information about it to productstatus.
    """

    CONFIG = {
        'EVA_THREDDS_POLL_INTERVAL': {
            'type': 'int',
            'help': 'How often should Thredds server be checked.',
            'default': '20',
        },
        'EVA_THREDDS_POLL_RETRIES': {
            'type': 'int',
            'help': 'Number of times to check for the data on Thredds server.',
            'default': '6',
        },
        'EVA_THREDDS_BASE_URL': {
            'type': 'string',
            'help': 'Base URL to prepend to the filename from the input resource',
            'default': '',
        },
    }

    REQUIRED_CONFIG = [
        'EVA_INPUT_PRODUCT',
        'EVA_INPUT_SERVICE_BACKEND',
        'EVA_OUTPUT_SERVICE_BACKEND',
        'EVA_THREDDS_BASE_URL',
    ]

    OPTIONAL_CONFIG = [
        'EVA_THREDDS_POLL_INTERVAL',
        'EVA_THREDDS_POLL_RETRIES',
    ]

    def init(self):
        """!
        @brief Populate internal variables.
        """
        self.output_service_backend = self.api.servicebackend[self.env['EVA_OUTPUT_SERVICE_BACKEND']]
        self.thredds_poll_interval = self.env['EVA_THREDDS_POLL_INTERVAL']
        self.thredds_poll_retries = self.env['EVA_THREDDS_POLL_RETRIES']
        self.thredds_base_url = self.env['EVA_THREDDS_BASE_URL']

    def create_job(self, message_id, resource):
        """!
        @brief Check if the resource is reachable via the provided URL if not, sleep and try again
        @fixme This job does not run on an executor, and is thus at least partially broken at the moment.
        """
        job = eva.job.Job(message_id, self.globe)

        # Assuming that when the .html link is accessible so will be the dataset via OPeNDAP
        basename = os.path.basename(resource.url)
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

        return job

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
