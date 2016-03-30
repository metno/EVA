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

    CONFIG = {
        'EVA_DELETE_INSTANCE_MAX': 'When determining data instances to delete, only look up the N latest instances, to prevent hammering the Productstatus server.',
    }

    REQUIRED_CONFIG = [
        'EVA_DELETE_INSTANCE_MAX',
    ]

    OPTIONAL_CONFIG = [
        'EVA_INPUT_DATA_FORMAT_UUID',
        'EVA_INPUT_PRODUCT_UUID',
        'EVA_INPUT_SERVICE_BACKEND_UUID',
    ]

    def init(self, *args, **kwargs):
        try:
            self.limit = int(self.env['EVA_DELETE_INSTANCE_MAX'])
            assert self.limit > 0
        except:
            raise eva.exceptions.InvalidConfigurationException(
                'EVA_DELETE_INSTANCE_MAX must be a positive, non-zero integer.'
            )

    def process_resource(self, resource):
        """
        @brief Look up and remove expired files.
        """

        # Get all expired datainstances for the product
        now = datetime.datetime.now().replace(tzinfo=dateutil.tz.tzutc())
        datainstances = self.api.datainstance.objects.filter(
            data__productinstance__product=resource.data.productinstance.product,
            expires__lte=now,
        ).order_by('-expires')

        self.logger.info("Found %d expired data instances" % datainstances.count())
        processed = self.limit

        for datainstance in datainstances:
            path = datainstance.url
            if path.startswith('file://'):
                path = path[7:]
            self.logger.info("%s: deleting expired file (EOL %s)", datainstance, datainstance.expires)

            job = eva.job.Job(self.logger)
            job.command = "#!/bin/bash\nrm -vf '%s'\n" % path
            self.execute(job)

            if job.status != eva.job.COMPLETE:
                raise eva.exceptions.RetryException("Executing deletion of '%s' failed." % resource.url)
            self.logger.info("The file '%s' has been permanently removed, or was already gone.", path)

            processed -= 1
            if processed == 0:
                break

        self.logger.info("All expired data instances successfully processed.")
