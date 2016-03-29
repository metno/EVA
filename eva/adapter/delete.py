import logging
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

    def unlink(self, path):
        if not os.path.exists(path):
            return False
        try:
            os.unlink(path)
        except OSError, e:
            raise eva.exceptions.RetryException(e)
        return True

    def process_resource(self, resource):
        """
        @brief Look up and remove expired files.
        """

        # Get all expired datainstances for the product
        now = datetime.datetime.now().replace(tzinfo=dateutil.tz.tzutc())
        datainstances = self.api.datainstance.objects.filter(
            data__productinstance__product=resource.data.productinstance.product,
            expires__lte=now,
        ).order_by('-expires').limit(int(self.env['EVA_DELETE_INSTANCE_MAX']))

        logging.info("Found %d expired data instances" % datainstances.count())
        for datainstance in datainstances:
            path = datainstance.url
            if path.startswith('file://'):
                path = path[7:]
            logging.info("%s: deleting expired file (EOL %s)", datainstance, datainstance.expires)
            if self.unlink(path):
                logging.info("The file '%s' has been permanently removed.", path)
            else:
                logging.warning("The file '%s' could not be found on the file system; may already be deleted.", path)
