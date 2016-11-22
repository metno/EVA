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
        'input_service_backend',
        'output_base_url',
    ]

    OPTIONAL_CONFIG = [
        'input_data_format',
        'input_partial',
        'input_product',
        'output_lifetime',
        'output_service_backend',
    ]

    PRODUCTSTATUS_REQUIRED_CONFIG = [
        'output_service_backend',
    ]

    def init(self):
        """!
        @brief Check that optional configuration is consistent.
        """
        if self.env['output_service_backend'] in self.env['input_service_backend']:
            raise eva.exceptions.InvalidConfigurationException('output_service_backend cannot be present in the list of input_service_backend, as that will result in an endless loop.')

    def create_job(self, job):
        """!
        @brief Create a Job object that will copy a file to another
        destination, and optionally post the result to Productstatus.
        """
        job.base_filename = os.path.basename(job.resource.url)
        job.input_file = eva.url_to_filename(job.resource.url)
        job.output_url = os.path.join(self.env['output_base_url'], job.base_filename)
        job.output_file = eva.url_to_filename(job.output_url)

        if self.post_to_productstatus():
            job.service_backend = self.api.servicebackend[self.env['output_service_backend']]
            # check if the destination file already exists
            qs = self.api.datainstance.objects.filter(url=job.output_url,
                                                      servicebackend=job.service_backend,
                                                      data=job.resource.data,
                                                      format=job.resource.format)
            if qs.count() != 0:
                raise eva.exceptions.JobNotGenerated("Destination URL '%s' already exists in Productstatus; this file has already been distributed." % job.output_url)

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
