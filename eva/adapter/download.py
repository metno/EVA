import os
import datetime

import eva
import eva.base.adapter
import eva.job
import eva.exceptions

import productstatus


class DownloadAdapter(eva.base.adapter.BaseAdapter):
    """
    An adapter that downloads files, and optionally posts their metadata to Productstatus.

    The consumer specifies Productstatus product, format, and service backend.
    The adapter triggers on DataInstance events, downloading matching files
    using wget.

    If the file is successfully downloaded, and the adapter has been configured
    with Productstatus credentials and the EVA_OUTPUT_* optional configuration,
    the file will be registered as a new DataInstance in Productstatus.
    """

    CONFIG = {
        'EVA_DOWNLOAD_DESTINATION': 'Where to place downloaded files',
    }

    REQUIRED_CONFIG = [
        'EVA_DOWNLOAD_DESTINATION',
        'EVA_INPUT_DATA_FORMAT_UUID',
        'EVA_INPUT_PRODUCT_UUID',
        'EVA_INPUT_SERVICE_BACKEND_UUID',
    ]

    OPTIONAL_CONFIG = [
        'EVA_OUTPUT_BASE_URL',
        'EVA_OUTPUT_LIFETIME',
        'EVA_OUTPUT_SERVICE_BACKEND_UUID',
    ]

    def init(self):
        """
        @brief Check that optional configuration is consistent.
        """
        if self.has_valid_output_config():
            self.post_to_productstatus = True
            self.require_productstatus_credentials()
            self.lifetime = datetime.timedelta(hours=int(self.env['EVA_OUTPUT_LIFETIME']))
            if self.env['EVA_OUTPUT_SERVICE_BACKEND_UUID'] in self.env['EVA_INPUT_SERVICE_BACKEND_UUID']:
                raise eva.exceptions.InvalidConfigurationException('EVA_OUTPUT_SERVICE_BACKEND_UUID cannot be present in the list of EVA_INPUT_SERVICE_BACKEND_UUID, as that will result in an endless loop.')
        else:
            self.post_to_productstatus = False
            self.logger.debug('Will not post any data to Productstatus.')

    def has_valid_output_config(self):
        """
        @return True if all optional output variables are configured, False otherwise.
        """
        return (
            (self.env['EVA_OUTPUT_BASE_URL'] is not None) and
            (self.env['EVA_OUTPUT_LIFETIME'] is not None) and
            (self.env['EVA_OUTPUT_SERVICE_BACKEND_UUID'] is not None)
        )

    def process_resource(self, resource):
        """
        @brief Download a file, and optionally post the result to Productstatus.
        """
        filename = os.path.basename(resource.url)
        job = eva.job.Job()
        job.command = """#!/bin/bash
        wget --no-verbose --output-document='%(destination)s' %(url)s
        """ % {
            'url': resource.url,
            'destination': os.path.join(self.env['EVA_DOWNLOAD_DESTINATION'], filename)
        }
        self.execute(job)

        if job.status != eva.job.COMPLETE:
            raise eva.exceptions.RetryException("Download of '%s' failed." % resource.url)

        if not self.post_to_productstatus:
            return

        self.logger.debug('Creating a new DataInstance on the Productstatus server...')
        datainstance = self.api.datainstance.create()
        datainstance.data = resource.data
        datainstance.format = resource.format
        datainstance.expires = datetime.datetime.now() + self.lifetime
        datainstance.servicebackend = self.api.servicebackend[self.env['EVA_OUTPUT_SERVICE_BACKEND_UUID']]
        datainstance.url = os.path.join(self.env['EVA_OUTPUT_BASE_URL'], filename)
        eva.retry_n(datainstance.save,
                    exceptions=(productstatus.exceptions.ServiceUnavailableException,),
                    give_up=0)
        self.logger.info('DataInstance %s, expires %s', datainstance, datainstance.expires)
        self.logger.info('The file %s has been successfully copied to %s', resource.url, datainstance.url)
