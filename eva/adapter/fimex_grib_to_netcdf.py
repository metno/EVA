import os.path

import eva
import eva.job
import eva.base.adapter

import productstatus.api


class FimexGRIB2NetCDFAdapter(eva.base.adapter.BaseAdapter):
    """!
    This adapter fills a NetCDF file with GRIB data using Fimex.
    The adapter requires an external library called `eva-adapter-support`.
    """
    CONFIG = {
        'fg2nc_lib': {
            'type': 'string',
            'help': 'Path to .../eva-adapter-support/FimexGRIB2NetCDFAdapter',
            'default': '',
        },
        'fg2nc_templatedir': {
            'type': 'string',
            'help': 'Path to the NetCDF template files required for this conversion',
            'default': '',
        },
    }

    REQUIRED_CONFIG = [
        'fg2nc_lib',
        'fg2nc_templatedir',
        'input_data_format',
        'input_product',
        'input_service_backend',
        'output_filename_pattern',
    ]

    OPTIONAL_CONFIG = [
        'input_partial',
        'output_base_url',
        'output_lifetime',
        'output_product',
        'output_service_backend',
    ]

    PRODUCTSTATUS_REQUIRED_CONFIG = [
        'output_base_url',
        'output_product',
        'output_service_backend',
    ]

    def init(self):
        for key in ['output_product', 'output_service_backend']:
            if key in self.env:
                setattr(self, key, self.env[key])

    def create_job(self, job):
        """!
        @brief Generate a Job which converts GRIB to NetCDF using the
        eva-adapter-support library.
        """
        reftime = job.resource.data.productinstance.reference_time

        job.data = {
            'reftime': reftime,
            'version': job.resource.data.productinstance.version,
            'time_period_begin': job.resource.data.time_period_begin,
            'time_period_end': job.resource.data.time_period_end,
            'filename': reftime.strftime(self.env['output_filename_pattern']),
        }

        job.command = """#!/bin/bash
#$ -S /bin/bash
{lib_fg2nc}/grib2nc \
--input "{gribfile}" \
--output "{destfile}" \
--reference_time "{reftime}" \
--template_directory "{templatedir}"
""".format(
            gribfile=eva.url_to_filename(job.resource.url),
            reftime=reftime.strftime("%Y-%m-%dT%H:%M:%S%z"),
            lib_fg2nc=self.env['fg2nc_lib'],
            templatedir=self.env['fg2nc_templatedir'],
            destfile=job.data['filename'],
        )

    def finish_job(self, job):
        if not job.complete():
            raise eva.exceptions.RetryException("GRIB to NetCDF conversion of '%s' failed." % job.resource.url)

        job.logger.info('Successfully filled the NetCDF file %s with data from %s', job.data['filename'], job.resource.url)

    def generate_resources(self, job, resources):
        """!
        @brief Generate a set of Productstatus resources based on job output.

        The adapter will try to re-use the existing ProductInstance if the
        input and output products are the same. Otherwise, it will create a new
        ProductInstance. Existing Data and DataInstance objects are re-used.
        """
        # Generate ProductInstance resource
        product_instance = productstatus.api.EvaluatedResource(
            self.api.productinstance.find_or_create_ephemeral,
            {
                'product': self.output_product,
                'reference_time': job.data['reftime'],
                'version': job.data['version'],
            }
        )
        resources['productinstance'] += [product_instance]

        # Generate Data resource
        data = productstatus.api.EvaluatedResource(
            self.api.data.find_or_create_ephemeral, {
                'productinstance': product_instance,
                'time_period_begin': job.data['time_period_begin'],
                'time_period_end': job.data['time_period_end'],
            }
        )
        resources['data'] = [data]

        # Generate DataInstance resource
        datainstance = self.api.datainstance.create()
        datainstance.data = data
        datainstance.partial = True
        datainstance.expires = self.expiry_from_lifetime()
        datainstance.format = self.api.dataformat['netcdf']
        datainstance.servicebackend = self.output_service_backend
        datainstance.url = os.path.join(self.env['output_base_url'], os.path.basename(job.data['filename']))
        resources['datainstance'] += [datainstance]
