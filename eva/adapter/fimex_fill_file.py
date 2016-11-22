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
        'fimex_fill_file_ncfill_path': {
            'type': 'string',
            'help': 'Path to the "ncfill" binary that will perform the fill operation.',
            'default': '',
        },
        'fimex_fill_file_template_directory': {
            'type': 'string',
            'help': 'Path to template directory that is used for fill operation configuration.',
            'default': '',
        }
    }

    REQUIRED_CONFIG = [
        'fimex_fill_file_ncfill_path',
        'fimex_fill_file_template_directory',
        'input_data_format',
        'input_product',
        'input_service_backend',
        'output_filename_pattern',
    ]

    OPTIONAL_CONFIG = [
        'input_partial',
        'output_base_url',
        'output_data_format',
        'output_lifetime',
        'output_product',
        'output_service_backend',
    ]

    PRODUCTSTATUS_REQUIRED_CONFIG = [
        'output_base_url',
        'output_data_format',
        'output_product',
        'output_service_backend',
    ]

    def init(self):
        if self.env['input_partial'] is not False:
            raise eva.exceptions.InvalidConfigurationException(
                'This adapter does not accept partial input files, and MUST be configured with input_partial=NO.'
            )
        for key in ['output_data_format', 'output_product', 'output_service_backend']:
            if key in self.env:
                setattr(self, key, self.env[key])
        self.ncfill_path = self.env['fimex_fill_file_ncfill_path']
        self.template_directory = self.template.from_string(self.env['fimex_fill_file_template_directory'])
        self.output_filename = self.template.from_string(self.env['output_filename_pattern'])

    def create_job(self, job):
        job.input_filename = eva.url_to_filename(job.resource.url)
        job.template_variables = {
            'datainstance': job.resource,
            'input_filename': os.path.basename(job.input_filename),
            'ncfill_path': self.ncfill_path,
            'template_directory': self.template_directory,
            'reference_time': job.resource.data.productinstance.reference_time,
        }

        # Render the Jinja2 templates and report any errors
        try:
            job.template_directory = self.template_directory.render(**job.template_variables)
            job.output_filename = self.output_filename.render(**job.template_variables)
        except Exception as e:
            raise eva.exceptions.InvalidConfigurationException(e)

        # Generate Fimex job
        command = ['#!/bin/bash']
        command += ['#$ -S /bin/bash']
        command += ["time %(ncfill)s --input '%(input)s' --output '%(output)s' --input_format '%(input_format)s' --reference_time '%(reference_time)s' --template_directory '%(template_directory)s'"]

        job.command = '\n'.join(command) + '\n'
        job.command = job.command % {
            'input': job.input_filename,
            'input_format': job.resource.format.slug,
            'ncfill': self.ncfill_path,
            'output': job.output_filename,
            'reference_time': eva.strftime_iso8601(job.resource.data.productinstance.reference_time),
            'template_directory': job.template_directory,
        }

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
        datainstance.url = os.path.join(self.env['output_base_url'], os.path.basename(job.output_filename))
        resources['datainstance'] += [datainstance]
