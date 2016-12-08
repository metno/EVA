import datetime
import dateutil.tz

import eva.exceptions
import eva.base.adapter
import eva.job


class DeleteAdapter(eva.base.adapter.BaseAdapter):
    """!
    @brief Remove expired files from file system.

    Find the latest expired data instances for given criteria, and remove the
    physical file from the file system.
    """

    REQUIRED_CONFIG = [
        'input_service_backend',
    ]

    OPTIONAL_CONFIG = [
        'input_partial',
        'input_product',
        'input_data_format',
    ]

    def create_job(self, job):
        """!
        @brief Look up expired files in Productstatus, and create a job that deletes them.

        One Job to rule them all,
        One Job to find them,
        One Job to bring them all and in the darkness bind them
        In the land of /dev/null, where the shadows lie.
        """

        # Get all expired datainstances for the product
        now = datetime.datetime.now().replace(tzinfo=dateutil.tz.tzutc())
        datainstances = self.api.datainstance.objects.filter(
            data__productinstance__product=job.resource.data.productinstance.product,
            format=job.resource.format,
            servicebackend=job.resource.servicebackend,
            expires__lte=now,
            deleted=False,
        ).order_by('-expires')

        count = datainstances.count()
        if count == 0:
            raise eva.exceptions.JobNotGenerated("No expired data instances matching this Data Instance's product, format, and service backend.")

        job.logger.info("Found %d expired data instances", count)

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
