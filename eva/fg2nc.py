import logging
import os.path

import eva.job
import eva.adapter


class FimexGribNetcdfAdapter(eva.adapter.JobQueueAdapter):
    def __init__(self, *args):
        super(FimexGribNetcdfAdapter, self).__init__("FimexGribNetcdfAdapter", *args)
        self.job_data = {}

    def get_env(self, key):
        klass = "FG2NC"
        return self.env['EVA_%s_%s' % (klass, key)]

    def match_event(self, event, resource):
        if event.resource != 'datainstance' or not resource:
            return []
        try:
            product_id = resource.data.productinstance.product.id
        except Exception, e:
            logging.warn("error retrieving product UUID", exc_info=e)
            return []
        expected_product_id = self.get_env('INPUT_PRODUCT_UUID')
        logging.debug("product_id: %s INPUT_PRODUCT_UUID=%s" % (product_id, expected_product_id))
        if product_id != expected_product_id:
            return []
        logging.debug("matching product uuid '%s'" % product_id)
        try:
            return self.create_job(event, resource)
        except Exception, e:
            logging.warn("error creating job", exc_info=e)
            return []

    def match_timeout(self):
        return []

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
            'lib_fg2nc': self.get_env('LIB'),
            'templatedir': self.get_env('TEMPLATEDIR'),
        }

        with open(os.path.join(params['templatedir'], 'pattern.nc'), 'r') as nc_pattern:
            job_data['nc_filename'] = reftime.strftime(nc_pattern.readline().strip())

        job.command = """
cd "{lib_fg2nc}"
./evaFimexFillNcFromGrib "{url}" "{gribfile}" "{reftime}" "{templatedir}"
""".format(**params)

        self.job_data[job.id] = job_data
        return self.enqueue_jobs([job])

    def finished_jobs(self, jobs):
        for job in jobs:
            if job.status == eva.job.COMPLETE:
                self.register_output(self.job_data[job.id])
            del self.job_data[job.id]

        return super(FimexGribNetcdfAdapter, self).finished_jobs(jobs)

    def register_output(self, job_data):
        productinstance = self.get_or_post_productinstance_resource(job_data)
        data = self.get_or_post_data_resource(productinstance, job_data)
        datainstance = self.post_datainstance_resource(data, job_data)
        logging.info("%s: registered datainstance %s", self.id, datainstance)

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
        output_product_uuid = self.get_env('OUTPUT_PRODUCT_UUID')
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
        resource.servicebackend = self.api.servicebackend[self.get_env('OUTPUT_SERVICE_BACKEND_UUID')]
        resource.url = self.get_env('OUTPUT_BASE_URL') + job_data['nc_filename']
        resource.save()
        return resource
