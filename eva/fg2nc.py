import logging
import os.path

import eva.job
import eva.adapter


class FimexGRIB2NetCDFAdapter(eva.adapter.BaseAdapter):
    REQUIRED_CONFIG = {
        'EVA_FG2NC_INPUT_PRODUCT_UUID': 'Productstatus Product UUID to process events for',
        'EVA_FG2NC_LIB': 'Path to .../EVA-adapter-support/FimexGribNetcdfAdapter',
        'EVA_FG2NC_OUTPUT_BASE_URL': 'Where to place the complete processed file',
        'EVA_FG2NC_OUTPUT_PRODUCT_UUID': 'Productstatus Product UUID for the finished product',
        'EVA_FG2NC_OUTPUT_SERVICE_BACKEND_UUID': 'Productstatus Service Backend UUID for the position of the finished product',
        'EVA_FG2NC_TEMPLATEDIR': 'Path to the NetCDF template files required for this conversion',
    }

    def process_event(self, event, resource):
        if event.resource != 'datainstance' or not resource:
            logging.info('Event is not of type DataInstance, ignoring.')
            return

        # FIXME: should have retry_n(...) or at least throw a RetryException
        product_id = resource.data.productinstance.product.id

        expected_product_id = self.env['EVA_FG2NC_INPUT_PRODUCT_UUID']
        if product_id != expected_product_id:
            logging.info('DataInstance Product UUID does not match configured value, ignoring.')
            return

        logging.info('DataInstance Product UUID matches configuration.')

        logging.info('Generating processing job.')
        job = self.create_job(event, resource)
        logging.info('Finished job generation, now executing.')

        self.executor.execute(job)

        if job.exit_code == 0:
            logging.info('Output file generated successfully, registering new DataInstance...')
            self.register_output(job.data)

    def create_job(self, event, datainstance):
        reftime = datainstance.data.productinstance.reference_time

        job = eva.job.Job()
        job_data = {
            'reftime': reftime,
            'version': datainstance.data.productinstance.version,
            'time_period_begin': datainstance.data.time_period_begin,
            'time_period_end': datainstance.data.time_period_end,
            'expires': datainstance.expires,
        }

        params = {
            'url': datainstance.url,
            'gribfile': (datainstance.id + '.grib'),
            'reftime': reftime.strftime("%Y-%m-%dT%H:%M:%S%z"),
            'lib_fg2nc': self.env['EVA_FG2NC_LIB'],
            'templatedir': self.env['EVA_FG2NC_TEMPLATEDIR'],
        }

        with open(os.path.join(params['templatedir'], 'pattern.nc'), 'r') as nc_pattern:
            job_data['nc_filename'] = reftime.strftime(nc_pattern.readline().strip())

        job.command = """
        set -e
        cd "{lib_fg2nc}"
        ./evaFimexFillNcFromGrib "{url}" "{gribfile}" "{reftime}" "{templatedir}"
        """.format(**params)
        job.data = job_data

        return job

    def register_output(self, job_data):
        productinstance = self.get_or_post_productinstance_resource(job_data)
        data = self.get_or_post_data_resource(productinstance, job_data)
        datainstance = self.post_datainstance_resource(data, job_data)
        logging.info("registered datainstance %s", datainstance)

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

    def get_or_post_productinstance_resource(self, job_data):
        """
        Return a matching ProductInstance resource according to Product, reference time and version.
        """
        # FIXME mostly copied from ecreceive.dataset
        product = self.get_productstatus_product()
        parameters = {
            'product': product,
            'reference_time': job_data['reftime'],
            'version': job_data['version'],  # FIXME is this the correct version?
        }
        return self.api.productinstance.find_or_create(parameters)

    def get_or_post_data_resource(self, productinstance, job_data):
        """
        Return a matching Data resource according to ProductInstance and data file
        begin/end times.
        """
        # FIXME mostly copied from ecreceive.dataset
        parameters = {
            'productinstance': productinstance,
            'time_period_begin': job_data['time_period_begin'],
            'time_period_end': job_data['time_period_end'],
        }
        return self.api.data.find_or_create(parameters)

    def post_datainstance_resource(self, data, job_data):
        """
        Create a DataInstance resource at the Productstatus server, referring to the
        given data set.
        """
        # FIXME mostly copied from ecreceive.dataset
        resource = self.api.datainstance.create()
        resource.data = data
        resource.expires = job_data['expires']
        resource.format = self.get_productstatus_dataformat("NetCDF")
        resource.servicebackend = self.api.servicebackend[self.env['EVA_FG2NC_OUTPUT_SERVICE_BACKEND_UUID']]
        resource.url = self.env['EVA_FG2NC_OUTPUT_BASE_URL'] + job_data['nc_filename']
        resource.save()
        return resource
