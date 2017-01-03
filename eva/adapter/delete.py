import datetime

import eva.exceptions
import eva.base.adapter
import eva.job


class DeleteAdapter(eva.base.adapter.BaseAdapter):
    """!
    @brief Remove expired files from file system.

    Find the latest expired data instances for given criteria, and remove the
    physical file from the file system.
    """

    CONFIG = {
        'delete_interval_secs': {
            'type': 'int',
            'default': '300',
        },
    }

    REQUIRED_CONFIG = [
        'input_service_backend',
    ]

    OPTIONAL_CONFIG = [
        'delete_interval_secs',
        'input_partial',
        'input_product',
        'input_data_format',
    ]

    def adapter_init(self):
        """
        Create objects required for adapter operation, and set the next delete run to the future.
        """
        self.products = set()
        self.formats = set()
        self.servicebackends = set()
        self.set_next_run()

    def set_next_run(self):
        """
        Set the earliest possible time for the next delete operation. The
        ``delete_interval_secs`` variable is used for this purpose.
        """
        self.next_run = eva.now_with_timezone() + datetime.timedelta(seconds=self.env['delete_interval_secs'])
        self.logger.info('Next delete operation will commence no earlier than %s', eva.strftime_iso8601(self.next_run))

    def add_params(self, product, format, servicebackend):
        """
        Add a product, data format, and service backend to the list of
        resources that should be queried for expired DataInstances when
        generating a job.
        """
        self.products.add(product.id)
        self.formats.add(format.id)
        self.servicebackends.add(servicebackend.id)

    def clear_params(self):
        """
        Clear product, data format, and service backend lists.
        """
        self.products.clear()
        self.formats.clear()
        self.servicebackends.clear()

    def ready_to_run(self):
        """
        Returns True if the next delete time set with `meth:set_next_run()` has
        been reached.
        """
        return self.next_run <= eva.now_with_timezone()

    def create_job(self, job):
        """!
        @brief Look up expired files in Productstatus, and create a job that deletes them.

        One Job to rule them all,
        One Job to find them,
        One Job to bring them all and in the darkness bind them
        In the land of /dev/null, where the shadows lie.
        """

        # Add datainstance parameters to delete list
        self.add_params(job.resource.data.productinstance.product, job.resource.format, job.resource.servicebackend)

        # Only create delete jobs once per `delete_interval_secs` seconds
        if not self.ready_to_run():
            time_diff = self.next_run - eva.now_with_timezone()
            seconds = time_diff.total_seconds()
            raise eva.exceptions.JobNotGenerated("Delete job has registered input data, but is not ready to run yet; must wait at least %d seconds." % seconds)

        # Get all expired datainstances
        self.logger.info('Collecting expired data instances.')
        self.logger.info('Products: %s', ', '.join(self.products))
        self.logger.info('Data formats: %s', ', '.join(self.formats))
        self.logger.info('Service backends: %s', ', '.join(self.servicebackends))
        datainstances = self.api.datainstance.objects.filter(
            data__productinstance__product__id__in=list(self.products),
            format__id__in=list(self.formats),
            servicebackend__id__in=list(self.servicebackends),
            expires__lte=eva.now_with_timezone(),
            deleted=False,
        ).order_by('expires')

        count = datainstances.count()
        if count == 0:
            self.clear_params()
            self.set_next_run()
            raise eva.exceptions.JobNotGenerated("No expired data instances matching stored product, format, or service backend.")

        job.logger.info("Found %d expired data instances.", count)

        job.command = ["#!/bin/bash"]

        # One line in delete script per data instance
        job.instance_list = []
        for datainstance in datainstances:
            job.instance_list.append(datainstance)
            path = datainstance.url
            if path.startswith('file://'):
                path = path[7:]
            job.logger.info("%s: expired at %s, queueing for deletion", datainstance, datainstance.expires)
            job.command += ["rm -vf '%s' && \\" % path]
            if datainstance.hash is not None and datainstance.hash_type == 'md5':
                # magical md5sum file deletion
                job.command += ["rm -vf '%s.md5' && \\" % path]
        job.command += ["exit 0"]

        job.command = '\n'.join(job.command) + '\n'

        self.clear_params()
        self.set_next_run()

    def finish_job(self, job):
        """!
        @brief Retry deletion on failures, and report metrics to statsd.
        """
        if not job.complete():
            raise eva.exceptions.RetryException("%s: deleting files failed." % job.resource)

        self.statsd.incr('eva_deleted_datainstances', len(job.instance_list))

    def generate_resources(self, job, resources):
        """!
        @brief Generate a set of Productstatus resources based on job output.

        This adapter will modify all DataInstance objects processed and set
        their 'deleted' property to True.
        """
        for datainstance in job.instance_list:
            datainstance.deleted = True
            resources['datainstance'] += [datainstance]
            job.logger.info('%s: marking DataInstance as deleted in Productstatus', datainstance)
