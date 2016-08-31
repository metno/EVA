import os
import datetime

import eva
import eva.base.adapter
import eva.job
import eva.exceptions
import eva.template

import productstatus


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
            self.logger.warning('Will not post any data to Productstatus.')
        self.in_opts = self.template.from_string(self.env['EVA_GRIDPP_INPUT_OPTIONS'])
        self.out_opts = self.template.from_string(self.env['EVA_GRIDPP_OUTPUT_OPTIONS'])
        self.generic_opts = self.template.from_string(self.env['EVA_GRIDPP_GENERIC_OPTIONS'])
        self.output_filename = self.template.from_string(self.env['EVA_OUTPUT_FILENAME_PATTERN'])

    def has_valid_output_config(self):
        """!
        @return True if all optional output variables are configured, False otherwise.
        """
        return (
            (self.env['EVA_OUTPUT_DATA_FORMAT'] is not None) and
            (self.env['EVA_OUTPUT_LIFETIME'] is not None) and
            (self.env['EVA_OUTPUT_PRODUCT'] is not None) and
            (self.env['EVA_OUTPUT_SERVICE_BACKEND'] is not None)
        )

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

        # Render the Jinja2 templates and report any errors
        try:
            params = {
                'input.file': filename,
                'input.options': self.in_opts.render(**template_variables),
                'output.file': self.output_filename.render(**template_variables),
                'output.options': self.out_opts.render(**template_variables),
                'generic.options': self.generic_opts.render(**template_variables),
            }
        except Exception as e:
            raise eva.exceptions.InvalidConfigurationException(e)

        # Generate and execute GridPP job
        job = eva.job.Job(message_id, self.logger)
        job.gridpp_params = params
        command = ["#!/bin/bash"]
        command += ["#$ -S /bin/bash"]
        command += ["set -e"]
        for module in self.env['EVA_GRIDPP_MODULES']:
            command += ["module load %s" % module]
        command += ["cp -v %(input.file)s %(output.file)s" % params]
        command += ["export OMP_NUM_THREADS=%d" % self.env['EVA_GRIDPP_THREADS']]
        command += ["gridpp %(input.file)s %(input.options)s %(output.file)s %(output.options)s %(generic.options)s" % params]
        job.command = '\n'.join(command) + '\n'

        return job

    def finish_job(self, job):
        # Retry on failure
        if job.status != eva.job.COMPLETE:
            raise eva.exceptions.RetryException(
                "GridPP post-processing of '%(input.file)s' to '%(output.file)s' failed." % params
            )

        # Succeed at this point if not posting to Productstatus
        if not self.post_to_productstatus:
            return

        resources = self.generate_resources(job)
        self.post_resources(resources, job)

    def get_or_post_productinstance_resource(self, resource):
        """!
        Return a matching ProductInstance resource according to Product, reference time and version.
        """
        product = self.output_product
        parameters = {
            'product': product,
            'reference_time': resource.data.productinstance.reference_time,
            'version': resource.data.productinstance.version,
        }
        return self.api.productinstance.find_or_create(parameters)

    def get_or_post_data_resource(self, product_instance, resource):
        """!
        Return a matching Data resource according to Productinstance and data begin/end times.
        """
        parameters = {
            'productinstance': product_instance,
            'time_period_begin': resource.data.time_period_begin,
            'time_period_end': resource.data.time_period_end
        }
        return self.api.data.find_or_create(parameters)

    def generate_resources(self, job):
        """!
        @brief Generate Productstatus resources based on finished job output.
        """
        resources = {
            'productinstance': [],
            'data': [],
            'datainstance': [],
        }

        product_instance = self.get_or_post_productinstance_resource(job.resource)
        resources['productinstance'] += [product_instance]

        data = self.get_or_post_data_resource(product_instance, job.resource)
        resources['data'] += [data]

        data_instance = self.api.datainstance.create()
        data_instance.data = data
        data_instance.url = 'file://' + job.gridpp_params['output.file']
        data_instance.servicebackend = self.output_service_backend
        data_instance.format = self.output_data_format
        data_instance.expires = self.expiry_from_lifetime()
        resources['datainstance'] += [data_instance]

        return resources
