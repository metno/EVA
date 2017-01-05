import os

import eva
import eva.job
import eva.base.adapter
import eva.tests.schemas

import productstatus.exceptions
import productstatus.api


class CWFAdapter(eva.base.adapter.BaseAdapter):
    """
    This adapter postprocesses ECMWF NetCDF files into output suitable for the
    EMEP and SNAP models.

    A special script called ``ecdis4cwf.sh`` is required for the conversion.
    The adapter provides the script with the correct input, and parses the
    script output when processing is finished.

    .. table::

       ===============================  ==============  ==================  ==========  ===========
       Variable                         Type            Default             Inclusion   Description
       ===============================  ==============  ==================  ==========  ===========
       cwf_domain                       |string|        NRPA_EUROPE_0_1     optional    Geographical domain to process.
       cwf_input_min_days               |int|           2                   optional    Number of forecast days required in source dataset.
       cwf_lifetime                     |list_int|      72,24               optional    Comma-separated list of DataInstance lifetimes. If the number of files
                                                                                        produced is less than the list size, the value of the last list instance is used for all subsequent files.
       cwf_modules                      |list_string|                       optional    Comma-separated list of modules to load before running.
       cwf_nml_data_format              |string|        nml                 optional    Which Productstatus data format to use for NML files.
       cwf_output_days                  |int|           3                   optional    Number of forecast days to generate in the resulting dataset.
       cwf_output_directory_pattern     |string|                            required    Destination output directory.
       cwf_parallel                     |int|           1                   optional    Number of processes run in parallel.
       cwf_script_path                  |string|                            required    Full path to the executable CWF script that should be run.
       ===============================  ==============  ==================  ==========  ===========
    """

    CONFIG = {
        'cwf_domain': {
            'type': 'string',
            'default': 'NRPA_EUROPE_0_1',
        },
        'cwf_input_min_days': {
            'type': 'int',
            'default': '2',
        },
        'cwf_output_days': {
            'type': 'int',
            'default': '3',
        },
        'cwf_output_directory_pattern': {
            'type': 'string',
            'default': '',
        },
        'cwf_parallel': {
            'type': 'int',
            'default': '1',
        },
        'cwf_script_path': {
            'type': 'string',
            'default': '',
        },
        'cwf_lifetime': {
            'type': 'list_int',
            'default': '72,24',
        },
        'cwf_modules': {
            'type': 'list_string',
            'default': '',
        },
        'cwf_nml_data_format': {
            'type': 'string',
            'default': 'nml',
        },
    }

    REQUIRED_CONFIG = [
        'cwf_output_directory_pattern',
        'cwf_script_path',
        'input_data_format',
        'input_product',
        'input_service_backend',
    ]

    OPTIONAL_CONFIG = [
        'cwf_domain',
        'cwf_input_min_days',
        'cwf_lifetime',
        'cwf_modules',
        'cwf_nml_data_format',
        'cwf_output_days',
        'cwf_parallel',
        'output_product',
        'output_service_backend',
        'output_data_format',
    ]

    PRODUCTSTATUS_REQUIRED_CONFIG = [
        'output_product',
        'output_service_backend',
        'output_data_format',
    ]

    def adapter_init(self, *args, **kwargs):
        if self.env['cwf_parallel'] < 1:
            raise eva.exceptions.InvalidConfigurationException(
                'Number of instances in cwf_parallel must be equal to or higher than 1.'
            )
        self.nml_data_format = self.api.dataformat[self.env['cwf_nml_data_format']]

    def is_netcdf_data_output(self, data):
        """
        Return True if the data entry parsed from a command line by
        CWFAdapter.parse_file_recognition_output is of type NetCDF, False
        otherwise.
        """
        return data['extension'] == '.nc'

    def is_nml_data_output(self, data):
        """
        Return True if the data entry parsed from a command line by
        CWFAdapter.parse_file_recognition_output is of type NML, False
        otherwise.
        """
        return data['extension'] == '.nml'

    def create_job(self, job):
        reference_time = job.resource.data.productinstance.reference_time

        # Skip processing if the destination data set already exists. This
        # disables re-runs and duplicates unless the DataInstance objects are
        # marked as deleted.
        if self.post_to_productstatus():
            qs = self.api.datainstance.objects.filter(data__productinstance__product=self.output_product,
                                                      data__productinstance__reference_time=reference_time,
                                                      servicebackend=self.output_service_backend,
                                                      deleted=False)
            if qs.count() != 0:
                raise eva.exceptions.JobNotGenerated("Destination data set already exists in Productstatus, skipping processing.")

        job.output_directory_template = self.template.from_string(
            self.env['cwf_output_directory_pattern']
        )
        job.output_directory = job.output_directory_template.render(
            reference_time=reference_time,
            domain=self.env['cwf_domain'],
        )

        cmd = []
        cmd += ['#/bin/bash']
        cmd += ['#$ -S /bin/bash']
        if self.env['cwf_parallel'] > 1:
            cmd += ['#$ -pe mpi-fn %d' % self.env['cwf_parallel']]
        for module in self.env['cwf_modules']:
            cmd += ['module load %s' % module]
        if self.env['cwf_parallel'] > 1:
            cmd += ['export ECDIS_PARALLEL=1']
        else:
            cmd += ['export ECDIS_PARALLEL=0']
        cmd += ['export DATE=%s' % reference_time.strftime('%Y%m%d')]
        cmd += ['export DOMAIN=%s' % self.env['cwf_domain']]
        cmd += ['export ECDIS=%s' % eva.url_to_filename(job.resource.url)]
        cmd += ['export ECDIS_TMPDIR=%s' % os.path.join(job.output_directory, 'work')]
        cmd += ['export NDAYS_MAX=%d' % self.env['cwf_output_days']]
        cmd += ['export NREC_DAY_MIN=%d' % self.env['cwf_input_min_days']]
        cmd += ['export OUTDIR=%s' % job.output_directory]
        cmd += ['export UTC=%s' % reference_time.strftime('%H')]
        cmd += ['%s >&2' % self.env['cwf_script_path']]

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

    def parse_file_recognition_output(self, lines):
        """
        Parse standard output containing time dimensions from NetCDF files into
        a structured format. Returns a list of dictionaries with file and time
        dimension information.
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

    def generate_resources(self, job, resources):
        """!
        @brief Generate a set of Productstatus resources based on job output.

        This adapter will re-use any existing ProductInstance and Data objects,
        and will create one new DataInstance resource for each file produced by
        the job.
        """
        product_instance = productstatus.api.EvaluatedResource(
            self.api.productinstance.find_or_create_ephemeral, {
                'product': self.output_product,
                'reference_time': job.resource.data.productinstance.reference_time,
                'version': job.resource.data.productinstance.version,
            }
        )
        resources['productinstance'] = [product_instance]

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

            data = productstatus.api.EvaluatedResource(self.api.data.find_or_create_ephemeral, parameters)
            resources['data'] += [data]

            data_instance = self.api.datainstance.create()
            data_instance.data = data
            data_instance.url = 'file://' + output_file['path']
            data_instance.servicebackend = self.output_service_backend

            if self.is_netcdf_data_output(output_file):
                data_instance.format = self.output_data_format
                if lifetime_index < len(self.env['cwf_lifetime']):
                    data_instance.expires = self.expiry_from_hours(self.env['cwf_lifetime'][lifetime_index])
                else:
                    data_instance.expires = self.expiry_from_hours(self.env['cwf_lifetime'][-1])
                lifetime_index += 1
            elif self.is_nml_data_output(output_file):
                data_instance.format = self.nml_data_format
                # let the NML file live as long as the shortest lived file in the output dataset
                data_instance.expires = self.expiry_from_hours(min(self.env['cwf_lifetime']))
            else:
                raise RuntimeError('Unsupported data format in job output: %s' % data['extension'])

            resources['datainstance'] += [data_instance]
