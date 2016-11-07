import os

import eva
import eva.base.adapter
import eva.job
import eva.exceptions


class DistributionAdapter(eva.base.adapter.BaseAdapter):
    """!
    @brief Copy data to another destination.

    This adapter distributes data files to other locations, and optionally
    posts their metadata to Productstatus.

    Note that at the moment, this adapter only supports copying files using the
    `cp` command, optionally on Lustre using the `lfs` utility to launch cp.
    """

    REQUIRED_CONFIG = [
        'EVA_INPUT_SERVICE_BACKEND',
        'EVA_OUTPUT_BASE_URL',
    ]

    OPTIONAL_CONFIG = [
        'EVA_INPUT_DATA_FORMAT',
        'EVA_INPUT_PARTIAL',
        'EVA_INPUT_PRODUCT',
        'EVA_OUTPUT_LIFETIME',
        'EVA_OUTPUT_SERVICE_BACKEND',
    ]

    PRODUCTSTATUS_REQUIRED_CONFIG = [
        'EVA_OUTPUT_SERVICE_BACKEND',
    ]

    def init(self):
        """!
        @brief Check that optional configuration is consistent.
        """
        if self.env['EVA_OUTPUT_SERVICE_BACKEND'] in self.env['EVA_INPUT_SERVICE_BACKEND']:
            raise eva.exceptions.InvalidConfigurationException('EVA_OUTPUT_SERVICE_BACKEND cannot be present in the list of EVA_INPUT_SERVICE_BACKEND, as that will result in an endless loop.')

    def create_job(self, message_id, resource):
        """!
        @brief Create a Job object that will copy a file to another
        destination, and optionally post the result to Productstatus.
        """
        job = eva.job.Job(message_id, self.globe)
        job.base_filename = os.path.basename(resource.url)
        job.input_file = eva.url_to_filename(resource.url)
        job.output_url = os.path.join(self.env['EVA_OUTPUT_BASE_URL'], job.base_filename)
        job.output_file = eva.url_to_filename(job.output_url)

        if self.post_to_productstatus():
            job.service_backend = self.api.servicebackend[self.env['EVA_OUTPUT_SERVICE_BACKEND']]
            # check if the destination file already exists
            qs = self.api.datainstance.objects.filter(url=job.output_url,
                                                      servicebackend=job.service_backend,
                                                      data=resource.data,
                                                      format=resource.format)
            if qs.count() != 0:
                job.logger.warning("Destination URL '%s' already exists in Productstatus; this file has already been distributed.", job.output_url)
                return

        lines = [
            "#!/bin/bash",
            "#$ -S /bin/bash",  # for GridEngine compatibility
            "`which lfs` cp --verbose %(source)s %(destination)s"
        ]
        values = {
            'source': job.input_file,
            'destination': job.output_file,
        }

        job.command = "\n".join(lines) + "\n"
        job.command = job.command % values

        return job

    def finish_job(self, job):
        if not job.complete():
            raise eva.exceptions.RetryException("Distribution of '%s' to '%s' failed." % (job.input_file, job.output_file))
        job.logger.info("The file '%s' has been successfully distributed to '%s'", job.input_file, job.output_file)

    def generate_resources(self, job, resources):
        """!
        @brief Generate a set of Productstatus resources based on job output.

        This adapter will post a new DataInstance using the same Data and
        ProductInstance as the input resource.
        """
        datainstance = self.api.datainstance.create()
        datainstance.data = job.resource.data
        datainstance.format = job.resource.format
        datainstance.expires = self.expiry_from_lifetime()
        datainstance.servicebackend = job.service_backend
        datainstance.url = job.output_url
        datainstance.hash = job.resource.hash
        datainstance.hash_type = job.resource.hash_type
        resources['datainstance'] += [datainstance]
