import os

import eva
import eva.base.adapter
import eva.job
import eva.exceptions
import eva.template


class FimexAdapter(eva.base.adapter.BaseAdapter):
    """!
    Generic FIMEX adapter that will accept virtually any parameter known to FIMEX.

    For flexibility, this adapter only takes three configuration options, that
    will allow users to set up any type of FIMEX job:

      * An output file name pattern
      * A generic command-line option string

    After generating the file, the adapter will post the information to
    Productstatus if the EVA_OUTPUT_* and EVA_PRODUCTSTATUS_* environments are
    specified.
    """

    CONFIG = {
        'EVA_FIMEX_PARAMETERS': {
            'type': 'string',
            'help': 'FIMEX command-line parameters.',
            'default': '',
        }
    }

    REQUIRED_CONFIG = [
        'EVA_FIMEX_PARAMETERS',
        'EVA_INPUT_DATA_FORMAT',
        'EVA_INPUT_PRODUCT',
        'EVA_INPUT_SERVICE_BACKEND',
        'EVA_OUTPUT_FILENAME_PATTERN',
    ]

    OPTIONAL_CONFIG = [
        'EVA_INPUT_PARTIAL',
        'EVA_OUTPUT_BASE_URL',
        'EVA_OUTPUT_DATA_FORMAT',
        'EVA_OUTPUT_LIFETIME',
        'EVA_OUTPUT_PRODUCT',
        'EVA_OUTPUT_SERVICE_BACKEND',
    ]

    def init(self):
        """!
        @brief Check that optional configuration is consistent.
        """
        if self.has_valid_output_config():
            self.post_to_productstatus = True
            self.require_productstatus_credentials()
            self.output_data_format = self.api.dataformat[self.env['EVA_OUTPUT_DATA_FORMAT']]
            self.output_product = self.api.product[self.env['EVA_OUTPUT_PRODUCT']]
            self.output_service_backend = self.api.servicebackend[self.env['EVA_OUTPUT_SERVICE_BACKEND']]
        else:
            self.post_to_productstatus = False
            self.logger.info('Will not post any data to Productstatus.')
        self.fimex_parameters = self.template.from_string(self.env['EVA_FIMEX_PARAMETERS'])
        self.output_filename = self.template.from_string(self.env['EVA_OUTPUT_FILENAME_PATTERN'])

    def has_valid_output_config(self):
        """!
        @return True if all optional output variables are configured, False otherwise.
        """
        return (
            (len(self.env['EVA_OUTPUT_BASE_URL']) > 0) and
            (len(self.env['EVA_OUTPUT_DATA_FORMAT']) > 0) and
            (len(self.env['EVA_OUTPUT_PRODUCT']) > 0) and
            (len(self.env['EVA_OUTPUT_SERVICE_BACKEND']) > 0)
        )

    def create_job(self, message_id, resource):
        """!
        @brief Create a generic FIMEX job.
        """
        job = eva.job.Job(message_id, self.logger)

        job.input_filename = eva.url_to_filename(resource.url)
        job.reference_time = resource.data.productinstance.reference_time
        template_variables = {
            'datainstance': resource,
            'input_filename': os.path.basename(job.input_filename),
            'reference_time': job.reference_time,
        }

        # Render the Jinja2 templates and report any errors
        try:
            params = self.fimex_parameters.render(**template_variables)
            job.output_filename = self.output_filename.render(**template_variables)
        except Exception as e:
            raise eva.exceptions.InvalidConfigurationException(e)

        # Generate Fimex job
        command = ['#!/bin/bash']
        command += ['#$ -S /bin/bash']
        command += ["time fimex --input.file '%(input.file)s' --output.file '%(output.file)s' %(params)s" % {
            'input.file': job.input_filename,
            'output.file': job.output_filename,
            'params': params,
        }]
        job.command = '\n'.join(command)

        return job

    def finish_job(self, job):
        # Retry on failure
        if not job.complete():
            raise eva.exceptions.RetryException(
                "Fimex conversion of '%s' to '%s' failed." % (job.input_filename, job.output_filename)
            )

        # Succeed at this point if not posting to Productstatus
        if not self.post_to_productstatus:
            return

        job.logger.info('Generating Productstatus resources...')
        resources = self.generate_resources(job)
        self.post_resources(resources, job)
        job.logger.info('Finished posting to Productstatus; all complete.')

    def generate_resources(self, job):
        resources = {
            'productinstance': [],
            'data': [],
            'datainstance': [],
        }

        # Generate ProductInstance resource
        parameters = {
            'product': self.output_product,
            'reference_time': job.resource.data.productinstance.reference_time,
        }
        if self.output_product == job.resource.data.productinstance.product:
            parameters['version'] = job.resource.data.productinstance.version

        product_instance = self.api.productinstance.find_or_create_ephemeral(parameters)
        resources['productinstance'] += [product_instance]

        # Generate Data resource
        data = self.api.data.find_or_create_ephemeral({
            'productinstance': product_instance,
            'time_period_begin': job.resource.data.time_period_begin,
            'time_period_end': job.resource.data.time_period_end,
        })
        resources['data'] += [data]

        # Generate DataInstance resource
        datainstance = self.api.datainstance.find_or_create_ephemeral({
            'data': data,
            'expires': self.expiry_from_lifetime(),
            'format': self.output_data_format,
            'servicebackend': self.output_service_backend,
            'url': os.path.join(self.env['EVA_OUTPUT_BASE_URL'], os.path.basename(job.output_filename)),
        })
        resources['datainstance'] += [datainstance]

        return resources
