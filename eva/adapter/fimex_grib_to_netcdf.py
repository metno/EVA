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
        'EVA_FG2NC_LIB': {
            'type': 'string',
            'help': 'Path to .../eva-adapter-support/FimexGRIB2NetCDFAdapter',
            'default': '',
        },
        'EVA_FG2NC_TEMPLATEDIR': {
            'type': 'string',
            'help': 'Path to the NetCDF template files required for this conversion',
            'default': '',
        },
    }

    REQUIRED_CONFIG = [
        'EVA_FG2NC_LIB',
        'EVA_FG2NC_TEMPLATEDIR',
        'EVA_INPUT_DATA_FORMAT',
        'EVA_INPUT_PRODUCT',
        'EVA_INPUT_SERVICE_BACKEND',
        'EVA_OUTPUT_FILENAME_PATTERN',
    ]

    OPTIONAL_CONFIG = [
        'EVA_INPUT_PARTIAL',
        'EVA_OUTPUT_BASE_URL',
        'EVA_OUTPUT_LIFETIME',
        'EVA_OUTPUT_PRODUCT',
        'EVA_OUTPUT_SERVICE_BACKEND',
    ]

    PRODUCTSTATUS_REQUIRED_CONFIG = [
        'EVA_OUTPUT_BASE_URL',
        'EVA_OUTPUT_PRODUCT',
        'EVA_OUTPUT_SERVICE_BACKEND',
    ]

    def init(self, *args, **kwargs):
        if self.post_to_productstatus():
            self.output_product = self.api.product[self.env['EVA_OUTPUT_PRODUCT']]
            self.output_service_backend = self.api.servicebackend[self.env['EVA_OUTPUT_SERVICE_BACKEND']]

    def create_job(self, message_id, resource):
        """!
        @brief Generate a Job which converts GRIB to NetCDF using the
        eva-adapter-support library.
        """
        job = eva.job.Job(message_id, self.logger)

        reftime = resource.data.productinstance.reference_time

        job.data = {
            'reftime': reftime,
            'version': resource.data.productinstance.version,
            'time_period_begin': resource.data.time_period_begin,
            'time_period_end': resource.data.time_period_end,
            'filename': reftime.strftime(self.env['EVA_OUTPUT_FILENAME_PATTERN']),
        }

        job.command = """#!/bin/bash
#$ -S /bin/bash
{lib_fg2nc}/grib2nc \
--input "{gribfile}" \
--output "{destfile}" \
--reference_time "{reftime}" \
--template_directory "{templatedir}"
""".format(
            gribfile=eva.url_to_filename(resource.url),
            reftime=reftime.strftime("%Y-%m-%dT%H:%M:%S%z"),
            lib_fg2nc=self.env['EVA_FG2NC_LIB'],
            templatedir=self.env['EVA_FG2NC_TEMPLATEDIR'],
            destfile=job.data['filename'],
        )

        return job

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
        datainstance.url = os.path.join(self.env['EVA_OUTPUT_BASE_URL'], os.path.basename(job.data['filename']))
        resources['datainstance'] += [datainstance]
