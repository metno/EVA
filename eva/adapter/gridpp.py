import eva
import eva.base.adapter
import eva.job
import eva.exceptions
import eva.template

import productstatus.api


class GridPPAdapter(eva.base.adapter.BaseAdapter):
    """!
    Generic GridPP adapter that will accept any parameter known to GridPP.

    The GridPP software is written by Thomas Nipen and is available at:
    https://github.com/metno/gridpp

    After generating the file, the adapter will post the information to
    Productstatus if the EVA_OUTPUT_* and EVA_PRODUCTSTATUS_* environments are
    specified.
    """

    CONFIG = {
        'EVA_GRIDPP_INPUT_OPTIONS': {
            'type': 'string',
            'help': 'GridPP command-line options for the input file.',
            'default': '',
        },
        'EVA_GRIDPP_OUTPUT_OPTIONS': {
            'type': 'string',
            'help': 'GridPP command-line options for the output file.',
            'default': '',
        },
        'EVA_GRIDPP_GENERIC_OPTIONS': {
            'type': 'string',
            'help': 'GridPP command-line options.',
            'default': '',
        },
        'EVA_GRIDPP_MODULES': {
            'type': 'list_string',
            'help': 'Comma-separated list of modules to load before running.',
            'default': '',
        },
        'EVA_GRIDPP_THREADS': {
            'type': 'int',
            'help': 'How many threads to use during calculations.',
            'default': '1',
        },
    }

    REQUIRED_CONFIG = [
        'EVA_INPUT_DATA_FORMAT',
        'EVA_INPUT_PRODUCT',
        'EVA_INPUT_SERVICE_BACKEND',
        'EVA_OUTPUT_FILENAME_PATTERN',
    ]

    OPTIONAL_CONFIG = [
        'EVA_GRIDPP_GENERIC_OPTIONS',
        'EVA_GRIDPP_INPUT_OPTIONS',
        'EVA_GRIDPP_MODULES',
        'EVA_GRIDPP_OUTPUT_OPTIONS',
        'EVA_GRIDPP_THREADS',
        'EVA_INPUT_PARTIAL',
        'EVA_OUTPUT_DATA_FORMAT',
        'EVA_OUTPUT_LIFETIME',
        'EVA_OUTPUT_PRODUCT',
        'EVA_OUTPUT_SERVICE_BACKEND',
    ]

    PRODUCTSTATUS_REQUIRED_CONFIG = [
        'EVA_OUTPUT_DATA_FORMAT',
        'EVA_OUTPUT_PRODUCT',
        'EVA_OUTPUT_SERVICE_BACKEND',
    ]

    def init(self):
        """!
        @brief Check that optional configuration is consistent.
        """
        if self.post_to_productstatus():
            self.output_data_format = self.api.dataformat[self.env['EVA_OUTPUT_DATA_FORMAT']]
            self.output_product = self.api.product[self.env['EVA_OUTPUT_PRODUCT']]
            self.output_service_backend = self.api.servicebackend[self.env['EVA_OUTPUT_SERVICE_BACKEND']]
        self.in_opts = self.template.from_string(self.env['EVA_GRIDPP_INPUT_OPTIONS'])
        self.out_opts = self.template.from_string(self.env['EVA_GRIDPP_OUTPUT_OPTIONS'])
        self.generic_opts = self.template.from_string(self.env['EVA_GRIDPP_GENERIC_OPTIONS'])
        self.output_filename = self.template.from_string(self.env['EVA_OUTPUT_FILENAME_PATTERN'])

    def create_job(self, message_id, resource):
        """!
        @brief Download a file, and optionally post the result to Productstatus.
        """
        filename = eva.url_to_filename(resource.url)
        reference_time = resource.data.productinstance.reference_time
        template_variables = {
            'reference_time': reference_time,
            'datainstance': resource,
        }

        job = eva.job.Job(message_id, self.globe)

        # Render the Jinja2 templates and report any errors
        try:
            job.gridpp_params = {
                'input.file': filename,
                'input.options': self.in_opts.render(**template_variables),
                'output.file': self.output_filename.render(**template_variables),
                'output.options': self.out_opts.render(**template_variables),
                'generic.options': self.generic_opts.render(**template_variables),
            }
        except Exception as e:
            raise eva.exceptions.InvalidConfigurationException(e)

        command = ["#!/bin/bash"]
        command += ["#$ -S /bin/bash"]
        command += ["set -e"]
        for module in self.env['EVA_GRIDPP_MODULES']:
            command += ["module load %s" % module]
        command += ["cp -v %(input.file)s %(output.file)s" % job.gridpp_params]
        command += ["export OMP_NUM_THREADS=%d" % self.env['EVA_GRIDPP_THREADS']]
        command += ["gridpp %(input.file)s %(input.options)s %(output.file)s %(output.options)s %(generic.options)s" % job.gridpp_params]
        job.command = '\n'.join(command) + '\n'

        return job

    def finish_job(self, job):
        """!
        @brief Retry on failure.
        """
        if not job.complete():
            raise eva.exceptions.RetryException(
                "GridPP post-processing of '%(input.file)s' to '%(output.file)s' failed." % job.gridpp_params
            )

    def generate_resources(self, job, resources):
        """!
        @brief Generate a set of Productstatus resources based on job output.

        This adapter will re-use any existing ProductInstance and Data
        resources, while creating a new DataInstance resource for the finished
        job output.
        """
        product_instance = productstatus.api.EvaluatedResource(
            self.api.productinstance.find_or_create_ephemeral, {
                'product': self.output_product,
                'reference_time': job.resource.data.productinstance.reference_time,
                'version': job.resource.data.productinstance.version,
            }
        )
        resources['productinstance'] += [product_instance]

        data = productstatus.api.EvaluatedResource(
            self.api.data.find_or_create_ephemeral, {
                'productinstance': product_instance,
                'time_period_begin': job.resource.data.time_period_begin,
                'time_period_end': job.resource.data.time_period_end
            }
        )
        resources['data'] += [data]

        data_instance = self.api.datainstance.create()
        data_instance.data = data
        data_instance.url = 'file://' + job.gridpp_params['output.file']
        data_instance.servicebackend = self.output_service_backend
        data_instance.format = self.output_data_format
        data_instance.expires = self.expiry_from_lifetime()
        resources['datainstance'] += [data_instance]
