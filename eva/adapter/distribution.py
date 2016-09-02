import os

import eva
import eva.base.adapter
import eva.job
import eva.exceptions

import productstatus


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

    def init(self):
        """!
        @brief Check that optional configuration is consistent.
        """
        if self.has_valid_output_config():
            self.post_to_productstatus = True
            self.require_productstatus_credentials()
            if self.env['EVA_OUTPUT_SERVICE_BACKEND'] in self.env['EVA_INPUT_SERVICE_BACKEND']:
                raise eva.exceptions.InvalidConfigurationException('EVA_OUTPUT_SERVICE_BACKEND cannot be present in the list of EVA_INPUT_SERVICE_BACKEND, as that will result in an endless loop.')
        else:
            self.post_to_productstatus = False
            self.logger.warning('Will not post any data to Productstatus.')

    def has_valid_output_config(self):
        """!
        @return True if all optional output variables are configured, False otherwise.
        """
        return (
            (self.env['EVA_OUTPUT_BASE_URL'] is not None) and
            (self.env['EVA_OUTPUT_SERVICE_BACKEND'] is not None)
        )

    def create_job(self, message_id, resource):
        """!
        @brief Create a Job object that will copy a file to another
        destination, and optionally post the result to Productstatus.
        """
        job = eva.job.Job(message_id, self.logger)
        job.base_filename = os.path.basename(resource.url)
        job.input_file = eva.url_to_filename(resource.url)
        job.output_url = os.path.join(self.env['EVA_OUTPUT_BASE_URL'], job.base_filename)
        job.output_file = eva.url_to_filename(job.output_url)

        if self.post_to_productstatus:
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

        if not self.post_to_productstatus:
            return

        job.logger.info('Creating a new DataInstance on the Productstatus server...')
        datainstance = self.api.datainstance.create()
        datainstance.data = job.resource.data
        datainstance.format = job.resource.format
        datainstance.expires = self.expiry_from_lifetime()
        datainstance.servicebackend = job.service_backend
        datainstance.url = job.output_url
        datainstance.hash = job.resource.hash
        datainstance.hash_type = job.resource.hash_type
        eva.retry_n(datainstance.save,
                    exceptions=(productstatus.exceptions.ServiceUnavailableException,),
                    give_up=0)
        job.logger.info('DataInstance %s, expires %s', datainstance, datainstance.expires)
        job.logger.info("The file '%s' has been successfully copied to '%s'", job.input_file, job.output_file)
