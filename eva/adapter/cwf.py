"""!
@brief ECDIS4CWF adapter.

This adapter postprocesses ECMWF NetCDF files into output suitable for the EMEP
and SNAP models, and posts the results to Productstatus.
"""

import os

import eva
import eva.job
import eva.base.adapter


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
        'EVA_CWF_NML_DATA_FORMAT': {
            'type': 'string',
            'help': 'Which Productstatus data format to use for NML files',
            'default': 'nml',
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
        'EVA_CWF_NML_DATA_FORMAT',
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
            self.nml_data_format = self.api.dataformat[self.env['EVA_CWF_NML_DATA_FORMAT']]

    def post_to_productstatus(self):
        return (len(self.env['EVA_OUTPUT_PRODUCT']) > 0 and
                len(self.env['EVA_OUTPUT_SERVICE_BACKEND']) > 0 and
                len(self.env['EVA_OUTPUT_DATA_FORMAT']) > 0)

    def is_netcdf_data_output(self, data):
        """!
        @brief Return True if the data entry parsed from a command line by
        CWFAdapter.parse_file_recognition_output is of type NetCDF, False otherwise.
        """
        return data['extension'] == '.nc'

    def is_nml_data_output(self, data):
        """!
        @brief Return True if the data entry parsed from a command line by
        CWFAdapter.parse_file_recognition_output is of type NML, False otherwise.
        """
        return data['extension'] == '.nml'

    def create_job(self, message_id, resource):
        reference_time = resource.data.productinstance.reference_time

        # Skip processing if the destination data set already exists. This
        # disables re-runs and duplicates unless the DataInstance objects are
        # marked as deleted.
        if self.post_to_productstatus():
            qs = self.api.datainstance.objects.filter(data__productinstance__product=self.output_product,
                                                      data__productinstance__reference_time=reference_time,
                                                      servicebackend=self.output_service_backend,
                                                      deleted=False)
            if qs.count() != 0:
                self.logger.warning("Destination data set already exists in Productstatus, skipping processing.")
                return

        job = eva.job.Job(message_id, self.logger)
        job.output_directory_template = self.template.from_string(
            self.env['EVA_CWF_OUTPUT_DIRECTORY_PATTERN']
        )
        job.output_directory = job.output_directory_template.render(
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
        cmd += ['export ECDIS_TMPDIR=%s' % os.path.join(job.output_directory, 'work')]
        cmd += ['export NDAYS_MAX=%d' % self.env['EVA_CWF_OUTPUT_DAYS']]
        cmd += ['export NREC_DAY_MIN=%d' % self.env['EVA_CWF_INPUT_MIN_DAYS']]
        cmd += ['export OUTDIR=%s' % job.output_directory]
        cmd += ['export UTC=%s' % reference_time.strftime('%H')]
        cmd += ['%s >&2' % self.env['EVA_CWF_SCRIPT_PATH']]

        # Run output recognition
        datestamp_glob = reference_time.strftime('*%Y%m%d_*.*')
        cmd += ['for file in %s; do' % os.path.join(job.output_directory, datestamp_glob)]
        cmd += ['    if [[ $file =~ \.nc$ ]]; then']
        cmd += ['        echo -n "$file "']
        cmd += ["        ncdump -l 1000 -t -v time $file | grep -E '^ ?time\s*='"]
        cmd += ['    elif [[ $file =~ \.nml$ ]]; then']
        cmd += ['        echo "$file"']
        cmd += ['    fi']
        cmd += ['done']

        job.command = "\n".join(cmd) + "\n"

        return job

    def finish_job(self, job):
        if not job.complete():
            raise eva.exceptions.RetryException(
                "Processing of %s to output directory %s failed." % (job.resource.url, job.output_directory)
            )

        try:
            job.output_files = self.parse_file_recognition_output(job.stdout)
        except:
            raise eva.exceptions.RetryException(
                "Processing of %s did not produce any legible output; expecting a list of file names and NetCDF time variables in standard output." % job.resource.url
            )

        if not self.post_to_productstatus():
            self.logger.info('NOT posting to Productstatus because of absent configuration.')

        job.logger.info('Generating Productstatus resources...')
        resources = self.generate_resources(job)
        self.post_resources(resources, job)
        job.logger.info('Finished posting to Productstatus; all complete.')

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
            data['extension'] = os.path.splitext(data['path'])[1]
            if self.is_netcdf_data_output(data):
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

    def generate_resources(self, job):
        """!
        @brief Generate Productstatus resources based on finished job output.
        """
        resources = {
            'productinstance': [],
            'data': [],
            'datainstance': [],
        }

        parameters = {
            'product': self.output_product,
            'reference_time': job.resource.data.productinstance.reference_time,
            'version': job.resource.data.productinstance.version,
        }
        product_instance = qs = self.api.productinstance.find_or_create_ephemeral(parameters)
        resources['productinstance'] += [product_instance]

        lifetime_index = 0

        for output_file in job.output_files:
            parameters = {
                'productinstance': product_instance,
            }

            if self.is_netcdf_data_output(output_file):
                parameters['time_period_begin'] = output_file['time_steps'][0]
                parameters['time_period_end'] = output_file['time_steps'][-1]
            else:
                parameters['time_period_begin'] = None
                parameters['time_period_end'] = None

            data = self.api.data.find_or_create_ephemeral(parameters)

            data = self.get_matching_data(resources['data'], data)
            resources['data'] += [data]

            data_instance = self.api.datainstance.create()
            data_instance.data = data
            data_instance.url = 'file://' + output_file['path']
            data_instance.servicebackend = self.output_service_backend

            if self.is_netcdf_data_output(output_file):
                data_instance.format = self.output_data_format
                if lifetime_index < len(self.env['EVA_CWF_LIFETIME']):
                    data_instance.expires = self.expiry_from_hours(self.env['EVA_CWF_LIFETIME'][lifetime_index])
                else:
                    data_instance.expires = self.expiry_from_hours(self.env['EVA_CWF_LIFETIME'][-1])
                lifetime_index += 1
            elif self.is_nml_data_output(output_file):
                data_instance.format = self.nml_data_format
                # let the NML file live as long as the shortest lived file in the output dataset
                data_instance.expires = self.expiry_from_hours(min(self.env['EVA_CWF_LIFETIME']))
            else:
                raise RuntimeError('Unsupported data format in job output: %s' % data['extension'])

            resources['datainstance'] += [data_instance]

        return resources
