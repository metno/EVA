import datetime
import os
import dateutil.tz

import eva.exceptions
import eva.base.adapter
import eva.job


class DeleteAdapter(eva.base.adapter.BaseAdapter):
    """
    @brief Remove expired files from file system.

    Find the latest expired data instances for given criteria, and remove the
    physical file from the file system.
    """

    REQUIRED_CONFIG = [
        'EVA_INPUT_SERVICE_BACKEND_UUID',
    ]

    OPTIONAL_CONFIG = [
        'EVA_INPUT_PRODUCT_UUID',
        'EVA_INPUT_DATA_FORMAT_UUID',
    ]

    def init(self, *args, **kwargs):
        self.require_productstatus_credentials()

    def process_resource(self, resource):
        """
        @brief Look up and remove expired files.
        """

        # Get all expired datainstances for the product
        now = datetime.datetime.now().replace(tzinfo=dateutil.tz.tzutc())
        datainstances = self.api.datainstance.objects.filter(
            data__productinstance__product=resource.data.productinstance.product,
            format=resource.format,
            servicebackend=resource.servicebackend,
            expires__lte=now,
            deleted=False,
        ).order_by('-expires')

        count = datainstances.count()
        if count == 0:
            self.logger.info("No expired data instances matching this Data Instance's product, format, and service backend.")
            return
        self.logger.info("Found %d expired data instances", count)

        job = eva.job.Job(self.logger)
        job.command = "#!/bin/bash\n"

        # One line in delete script per data instance
        instance_list = []
        for datainstance in datainstances:
            instance_list.append(datainstance)
            path = datainstance.url
            if path.startswith('file://'):
                path = path[7:]
            self.logger.info("%s: expired at %s, queueing for deletion", datainstance.expires, datainstance)
            job.command += "rm -vf '%s' && \\\n" % path

        job.command += "exit 0\n"
        self.logger.info(job.command)
        self.execute(job)

        if job.status != eva.job.COMPLETE:
            raise eva.exceptions.RetryException("%s: deleting files failed." % resource)

        for datainstance in instance_list:
            datainstance.deleted = True
            datainstance.save()
            self.logger.info('%s: marked DataInstance as deleted in Productstatus', datainstance)

        self.logger.info("All expired data instances successfully processed.")
