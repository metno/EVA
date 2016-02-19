import logging
import os.path

import eva.job
import eva.adapter


class FimexGRIB2NetCDFAdapter(eva.adapter.BaseAdapter):
    REQUIRED_CONFIG = {
        'EVA_FG2NC_INPUT_DATA_FORMAT_UUID': 'Productstatus data format UUID to filter by',
        'EVA_FG2NC_INPUT_PRODUCT_UUID': 'Productstatus Product UUID to process events for',
        'EVA_FG2NC_INPUT_SERVICE_BACKEND_UUID': 'Productstatus service backend UUID to filter by',
        'EVA_FG2NC_LIB': 'Path to .../eva-adapter-support/FimexGRIB2NetCDFAdapter',
        'EVA_FG2NC_OUTPUT_BASE_URL': 'Where to place the complete processed file',
        'EVA_FG2NC_OUTPUT_PATTERN': 'strftime pattern for NetCDF output filename',
        'EVA_FG2NC_OUTPUT_PRODUCT_UUID': 'Productstatus Product UUID for the finished product',
        'EVA_FG2NC_OUTPUT_SERVICE_BACKEND_UUID': 'Productstatus Service Backend UUID for the position of the finished product',
        'EVA_FG2NC_TEMPLATEDIR': 'Path to the NetCDF template files required for this conversion',
    }

    def process_resource(self, resource):
        if resource._collection._resource_name != 'datainstance':
            logging.info('Resource is not of type DataInstance, ignoring.')
            return

        # FIXME: Productstatus lookups should have retry_n(...) or at least throw a RetryException
        if resource.data.productinstance.product.id != self.env['EVA_FG2NC_INPUT_PRODUCT_UUID']:
            logging.info('DataInstance Product UUID does not match configured value, ignoring.')
            return

        # FIXME
        if resource.format.id != self.env['EVA_FG2NC_INPUT_DATA_FORMAT_UUID']:
            logging.info('Data format %s does not match configured value, ignoring.', resource.format.name)
            return

        # FIXME
        if resource.servicebackend.id != self.env['EVA_FG2NC_INPUT_SERVICE_BACKEND_UUID']:
            logging.info('Service backend %s does not match configured value, ignoring.', resource.servicebackend.name)
            return

        logging.info('DataInstance matches configuration.')

        logging.info('Generating processing job.')
        job = self.create_job(resource)
        logging.info('Finished job generation, now executing.')

        self.executor.execute(job)

        if job.exit_code == 0:
            logging.info('Output file generated successfully, registering new DataInstance...')
            self.register_output(job)

    def url_to_filename(self, url):
        """
        @brief Convert a file://... URL to a path name. Raises an exception if
        the URL does not start with file://.
        """
        start = 'file://'
        if not url.startswith(start):
            raise RuntimeError('Expected an URL starting with %s, got %s instead' % (start, url))
        return url[len(start):]

    def create_job(self, datainstance):
        reftime = datainstance.data.productinstance.reference_time

        job = eva.job.Job()
        job.data = {
            'reftime': reftime,
            'version': datainstance.data.productinstance.version,
            'time_period_begin': datainstance.data.time_period_begin,
            'time_period_end': datainstance.data.time_period_end,
            'expires': datainstance.expires,
            'filename': reftime.strftime(self.env['EVA_FG2NC_OUTPUT_PATTERN']),
        }

        params = {
            'gribfile': self.url_to_filename(datainstance.url),
            'reftime': reftime.strftime("%Y-%m-%dT%H:%M:%S%z"),
            'lib_fg2nc': self.env['EVA_FG2NC_LIB'],
            'templatedir': self.env['EVA_FG2NC_TEMPLATEDIR'],
            'destfile': os.path.join(self.env['EVA_FG2NC_OUTPUT_DESTINATION'], job.data['filename'])
        }

        job.command = """#!/bin/bash
        {lib_fg2nc}/grib2nc \
            --input "{gribfile}" \
            --output "{destfile}" \
            --reference_time "{reftime}" \
            --template_directory "{templatedir}"
        """.format(**params)

        return job

    def register_output(self, job):
        productinstance = self.get_or_post_productinstance_resource(job)
        data = self.get_or_post_data_resource(productinstance, job)
        datainstance = self.post_datainstance_resource(data, job)
        logging.info("Registered DataInstance: %s", datainstance)

    def get_productstatus_dataformat(self, file_type):
        """
        Given a file type string, return a DataFormat object pointing to the
        correct data format.
        """
        # FIXME copied from ecreceive.dataset, except for parameter and exception
        qs = self.api.dataformat.objects.filter(name=file_type)
        if qs.count() == 0:
            raise Exception(
                "Data format '%s' was not found on the Productstatus server" % file_type
            )
        resource = qs[0]
        logging.info('%s: Productstatus dataformat for %s' % (resource, file_type))
        return resource

    def get_productstatus_product(self):
        output_product_uuid = self.env['EVA_FG2NC_OUTPUT_PRODUCT_UUID']
        return self.api.product[output_product_uuid]

    def get_or_post_productinstance_resource(self, job):
        """
        Return a matching ProductInstance resource according to Product, reference time and version.
        """
        # FIXME mostly copied from ecreceive.dataset
        product = self.get_productstatus_product()
        parameters = {
            'product': product,
            'reference_time': job.data['reftime'],
            'version': job.data['version'],  # FIXME is this the correct version?
        }
        return self.api.productinstance.find_or_create(parameters)

    def get_or_post_data_resource(self, productinstance, job):
        """
        Return a matching Data resource according to ProductInstance and data file
        begin/end times.
        """
        # FIXME mostly copied from ecreceive.dataset
        parameters = {
            'productinstance': productinstance,
            'time_period_begin': job.data['time_period_begin'],
            'time_period_end': job.data['time_period_end'],
        }
        return self.api.data.find_or_create(parameters)

    def post_datainstance_resource(self, data, job):
        """
        Create a DataInstance resource at the Productstatus server, referring to the
        given data set.
        """
        # FIXME mostly copied from ecreceive.dataset
        resource = self.api.datainstance.create()
        resource.data = data
        resource.expires = job.data['expires']
        resource.format = self.get_productstatus_dataformat("NetCDF")
        resource.servicebackend = self.api.servicebackend[self.env['EVA_FG2NC_OUTPUT_SERVICE_BACKEND_UUID']]
        resource.url = os.path.join(self.env['EVA_FG2NC_OUTPUT_BASE_URL'], job.data['filename'])
        resource.save()
        return resource
