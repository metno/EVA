"""!
@brief ECDIS4CWF adapter.

This adapter postprocesses ECMWF NetCDF files into output suitable for the EMEP
and SNAP models, and posts the results to Productstatus.
"""

import os

import eva
import eva.job
import eva.base.adapter

import productstatus.exceptions


class CWFAdapter(eva.base.adapter.BaseAdapter):
    CONFIG = {
        'EVA_CWF_DOMAIN': {
            'type': 'string',
            'help': 'Geographical domain to process.',
            'default': 'NRPA_EUROPE_0_1',
        },
        'EVA_CWF_INPUT_MIN_DAYS': {
            'type': 'int',
            'help': 'Number of forecast days required in source dataset.',
            'default': '2',
        },
        'EVA_CWF_OUTPUT_DAYS': {
            'type': 'int',
            'help': 'Number of forecast days to generate in the resulting dataset.',
            'default': '3',
        },
        'EVA_CWF_OUTPUT_DIRECTORY_PATTERN': {
            'type': 'string',
            'help': 'Destination output directory',
            'default': '',
        },
        'EVA_CWF_PARALLEL': {
            'type': 'int',
            'help': 'Number of processes run in parallel.',
            'default': '1',
        },
        'EVA_CWF_SCRIPT_PATH': {
            'type': 'string',
            'help': 'Full path to the executable CWF script that should be run.',
            'default': '',
        },
        'EVA_CWF_LIFETIME': {
            'type': 'list_int',
            'help': 'Comma-separated list of DataInstance lifetimes. If the number of files produced is less than the list size, the value of the last list instance is used for all subsequent files.',
            'default': '72,24',
        },
        'EVA_CWF_MODULES': {
            'type': 'list_string',
            'help': 'Comma-separated list of modules to load before running.',
            'default': '',
        },
    }

    REQUIRED_CONFIG = [
        'EVA_CWF_OUTPUT_DIRECTORY_PATTERN',
        'EVA_CWF_SCRIPT_PATH',
        'EVA_INPUT_DATA_FORMAT',
        'EVA_INPUT_PRODUCT',
        'EVA_INPUT_SERVICE_BACKEND',
    ]

    OPTIONAL_CONFIG = [
        'EVA_CWF_DOMAIN',
        'EVA_CWF_INPUT_MIN_DAYS',
        'EVA_CWF_LIFETIME',
        'EVA_CWF_MODULES',
        'EVA_CWF_OUTPUT_DAYS',
        'EVA_CWF_PARALLEL',
        'EVA_OUTPUT_PRODUCT',
        'EVA_OUTPUT_SERVICE_BACKEND',
        'EVA_OUTPUT_DATA_FORMAT',
    ]

    def init(self, *args, **kwargs):
        if self.env['EVA_CWF_PARALLEL'] < 1:
            raise eva.exceptions.InvalidConfigurationException(
                'Number of instances in EVA_CWF_PARALLEL must be equal to or higher than 1.'
            )
        if self.post_to_productstatus():
            self.output_product = self.api.product[self.env['EVA_OUTPUT_PRODUCT']]
            self.output_service_backend = self.api.servicebackend[self.env['EVA_OUTPUT_SERVICE_BACKEND']]
            self.output_data_format = self.api.dataformat[self.env['EVA_OUTPUT_DATA_FORMAT']]

    def post_to_productstatus(self):
        return (len(self.env['EVA_OUTPUT_PRODUCT']) > 0 and
                len(self.env['EVA_OUTPUT_SERVICE_BACKEND']) > 0 and
                len(self.env['EVA_OUTPUT_DATA_FORMAT']) > 0)

    def process_resource(self, message_id, resource):
        reference_time = resource.data.productinstance.reference_time
        output_directory_template = self.template.from_string(
            self.env['EVA_CWF_OUTPUT_DIRECTORY_PATTERN']
        )
        output_directory = output_directory_template.render(
            reference_time=reference_time,
            domain=self.env['EVA_CWF_DOMAIN'],
        )

        cmd = []
        cmd += ['#/bin/bash']
        cmd += ['#$ -S /bin/bash']
        if self.env['EVA_CWF_PARALLEL'] > 1:
            cmd += ['#$ -pe mpi-fn %d' % self.env['EVA_CWF_PARALLEL']]
        for module in self.env['EVA_CWF_MODULES']:
            cmd += ['module load %s' % module]
        if self.env['EVA_CWF_PARALLEL'] > 1:
            cmd += ['export ECDIS_PARALLEL=1']
        else:
            cmd += ['export ECDIS_PARALLEL=0']
        cmd += ['export DATE=%s' % reference_time.strftime('%Y%m%d')]
        cmd += ['export DOMAIN=%s' % self.env['EVA_CWF_DOMAIN']]
        cmd += ['export ECDIS=%s' % eva.url_to_filename(resource.url)]
        cmd += ['export ECDIS_TMPDIR=%s' % os.path.join(output_directory, 'work')]
        cmd += ['export NDAYS_MAX=%d' % self.env['EVA_CWF_OUTPUT_DAYS']]
        cmd += ['export NREC_DAY_MIN=%d' % self.env['EVA_CWF_INPUT_MIN_DAYS']]
        cmd += ['export OUTDIR=%s' % output_directory]
        cmd += ['export UTC=%s' % reference_time.strftime('%H')]
        cmd += ['%s >&2' % self.env['EVA_CWF_SCRIPT_PATH']]

        # Run output recognition
        datestamp_glob = reference_time.strftime('*%Y%m%d_*.nc')
        cmd += ['for file in %s; do' % os.path.join(output_directory, datestamp_glob)]
        cmd += ['    echo -n "$file "']
        cmd += ["    ncdump -l 1000 -t -v time $file | grep -E '^ ?time\s*='"]
        cmd += ['done']

        job = eva.job.Job(message_id, self.logger)
        job.command = "\n".join(cmd) + "\n"
        job.resource = resource
        self.execute(job)

        if job.status != eva.job.COMPLETE:
            raise eva.exceptions.RetryException(
                "Processing of %s to output directory %s failed." % (resource.url, output_directory)
            )

        try:
            job.output_files = self.parse_file_recognition_output(job.stdout)
        except:
            raise eva.exceptions.RetryException(
                "Processing of %s did not produce any legible output; expecting a list of file names and NetCDF time variables in standard output." % resource.url
            )

        if not self.post_to_productstatus():
            self.logger.info('NOT posting to Productstatus because of absent configuration.')

        self.logger.info('Posting information about new dataset to Productstatus.')
        resources = self.generate_resources(resource, job)
        self.post_resources(resources)
        self.logger.info('Finished posting to Productstatus; job complete.')

    def parse_file_recognition_output(self, lines):
        """!
        @brief Parse standard output containing time dimensions from NetCDF
        files into a structured format.
        @returns A list of dictionaries with file and time dimension information.
        """
        result = []
        for line in lines:
            # Each output line looks like this:
            # /tmp/meteo20160606_00.nc  time = "2016-06-06 12", "2016-06-06 15", "2016-06-06 18", "2016-06-06 21", "2016-06-07" ;
            if len(line) < 5:
                continue
            data = {}
            tokens = line.split()
            data['path'] = tokens[0]
            time_str = ' '.join(tokens[3:-1])
            times = sorted([eva.netcdf_time_to_timestamp(x.strip(' "')) for x in time_str.split(',')])
            data['time_steps'] = times
            result += [data]
        return result

    def get_matching_data(self, data_list, data):
        """!
        @brief Return a Data resource if one matching the data variable is
        found in data_list, else return the original object.
        """
        for m_data in data_list:
            if data.productinstance != m_data.productinstance:
                continue
            if data.time_period_begin != m_data.time_period_begin:
                continue
            if data.time_period_end != m_data.time_period_end:
                continue
            return m_data
        return data

    def generate_resources(self, resource, job):
        """!
        @brief Generate Productstatus resources based on finished job output.
        """
        resources = {
            'productinstance': [],
            'data': [],
            'datainstance': [],
        }

        product_instance = self.api.productinstance.create()
        product_instance.product = self.output_product
        product_instance.reference_time = resource.data.productinstance.reference_time

        resources['productinstance'] += [product_instance]

        lifetime_index = 0

        for output_file in job.output_files:
            data = self.api.data.create()
            data.productinstance = product_instance
            data.time_period_begin = output_file['time_steps'][0]
            data.time_period_end = output_file['time_steps'][-1]

            data = self.get_matching_data(resources['data'], data)
            resources['data'] += [data]

            data_instance = self.api.datainstance.create()
            data_instance.data = data
            data_instance.url = 'file://' + output_file['path']
            data_instance.servicebackend = self.output_service_backend
            data_instance.format = self.output_data_format
            if lifetime_index < len(self.env['EVA_CWF_LIFETIME']):
                data_instance.expires = self.expiry_from_hours(self.env['EVA_CWF_LIFETIME'][lifetime_index])
            else:
                data_instance.expires = self.expiry_from_hours(self.env['EVA_CWF_LIFETIME'][-1])

            resources['datainstance'] += [data_instance]

            lifetime_index += 1

        return resources
