import os

import eva
import eva.base.adapter
import eva.job
import eva.exceptions
import eva.template


class FimexFillFileAdapter(eva.base.adapter.BaseAdapter):
    """!
    FIMEX adapter that will fill a template file with data from other files.
    """

    CONFIG = {
        'EVA_FIMEX_FILL_FILE_TEMPLATE': {
            'type': 'string',
            'help': 'Path to template file that is used as a starting point for the fill operation.',
            'default': '',
        }
    }

    REQUIRED_CONFIG = [
        'EVA_FIMEX_FILL_FILE_TEMPLATE',
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

    PRODUCTSTATUS_REQUIRED_CONFIG = [
        'EVA_OUTPUT_BASE_URL',
        'EVA_OUTPUT_DATA_FORMAT',
        'EVA_OUTPUT_PRODUCT',
        'EVA_OUTPUT_SERVICE_BACKEND',
    ]

    def init(self):
        if self.env['EVA_INPUT_PARTIAL'] is not False:
            raise eva.exceptions.InvalidConfigurationException(
                'This adapter does not accept partial input files, and MUST be configured with EVA_INPUT_PARTIAL=NO.'
            )
        if self.post_to_productstatus():
            self.output_data_format = self.api.dataformat[self.env['EVA_OUTPUT_DATA_FORMAT']]
            self.output_product = self.api.product[self.env['EVA_OUTPUT_PRODUCT']]
            self.output_service_backend = self.api.servicebackend[self.env['EVA_OUTPUT_SERVICE_BACKEND']]
        self.fill_file_template = self.template.from_string(self.env['EVA_FIMEX_FILL_FILE_TEMPLATE'])
        self.output_filename = self.template.from_string(self.env['EVA_OUTPUT_FILENAME_PATTERN'])

    def create_job(self, message_id, resource):
        job = eva.job.Job(message_id, self.logger)

        job.input_filename = eva.url_to_filename(resource.url)
        job.template_variables = {
            'datainstance': resource,
            'input_filename': os.path.basename(job.input_filename),
            'reference_time': resource.data.productinstance.reference_time,
        }

        # Render the Jinja2 templates and report any errors
        try:
            job.fill_file_template = self.fill_file_template.render(**job.template_variables)
            job.output_filename = self.output_filename.render(**job.template_variables)
        except Exception as e:
            raise eva.exceptions.InvalidConfigurationException(e)

        # Generate Fimex job
        command = ['#!/bin/bash']
        command += ['#$ -S /bin/bash']
        command += ["[ ! -f '%(output.fillFile)s' ] && cp -v '%(template)s' '%(output.fillFile)s'"]
        command += ["time fimex --input.file '%(input.file)s' --output.fillFile '%(output.fillFile)s'"]

        job.command = '\n'.join(command) + '\n'
        job.command = job.command % {
            'input.file': job.input_filename,
            'output.fillFile': job.output_filename,
            'template': job.fill_file_template,
        }

        return job

    def finish_job(self, job):
        """!
        @brief Retry on failures.
        """
        if not job.complete():
            raise eva.exceptions.RetryException(
                "FIMEX could not fill file '%s' with data from '%s'." % (job.output_filename, job.input_filename)
            )

    def generate_resources(self, job, resources):
        """!
        @brief Generate a set of Productstatus resources based on job output.

        The adapter will re-use the existing ProductInstance and Data
        resources, and create a new DataInstance resource. The DataInstance
        resource will be marked as "partial".
        """
        # Generate DataInstance resource
        datainstance = self.api.datainstance.create()
        datainstance.data = job.resource.data
        datainstance.partial = True
        datainstance.expires = self.expiry_from_lifetime()
        datainstance.format = self.output_data_format
        datainstance.servicebackend = self.output_service_backend
        datainstance.url = os.path.join(self.env['EVA_OUTPUT_BASE_URL'], os.path.basename(job.output_filename))
        resources['datainstance'] += [datainstance]
