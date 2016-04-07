import os
import datetime

import eva
import eva.base.adapter
import eva.job
import eva.exceptions

import productstatus


class DownloadAdapter(eva.base.adapter.BaseAdapter):
    """!
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
        'EVA_DOWNLOAD_CHECK_HASH': 'Whether or not to check the hash of downloaded files against the Productstatus hash. Defaults to YES.',
    }

    REQUIRED_CONFIG = [
        'EVA_DOWNLOAD_DESTINATION',
        'EVA_INPUT_DATA_FORMAT_UUID',
        'EVA_INPUT_PRODUCT_UUID',
        'EVA_INPUT_SERVICE_BACKEND_UUID',
    ]

    OPTIONAL_CONFIG = [
        'EVA_DOWNLOAD_CHECK_HASH',
        'EVA_INPUT_PARTIAL',
        'EVA_OUTPUT_BASE_URL',
        'EVA_OUTPUT_LIFETIME',
        'EVA_OUTPUT_SERVICE_BACKEND_UUID',
    ]

    def init(self):
        """!
        @brief Check that optional configuration is consistent.
        """
        if self.env['EVA_DOWNLOAD_CHECK_HASH'] is not None:
            self.check_hash = eva.parse_boolean_string(self.env['EVA_DOWNLOAD_CHECK_HASH'])
            if self.check_hash is None:
                self.check_hash = True
        else:
            self.check_hash = False
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
        """!
        @return True if all optional output variables are configured, False otherwise.
        """
        return (
            (self.env['EVA_OUTPUT_BASE_URL'] is not None) and
            (self.env['EVA_OUTPUT_LIFETIME'] is not None) and
            (self.env['EVA_OUTPUT_SERVICE_BACKEND_UUID'] is not None)
        )

    def process_resource(self, message_id, resource):
        """!
        @brief Download a file, and optionally post the result to Productstatus.
        """
        filename = os.path.basename(resource.url)
        job = eva.job.Job(message_id, self.logger)

        lines = [
            "#!/bin/bash",
            "#$ -S /bin/bash",  # for GridEngine compatibility
            "wget --no-verbose --output-document='%(destination)s' %(url)s",
        ]
        values = {
            'url': resource.url,
            'destination': os.path.join(self.env['EVA_DOWNLOAD_DESTINATION'], filename),
        }

        if resource.hash:
            if resource.hash_type == 'md5':
                job.logger.info(
                    "Will check downloaded file against %s hash sum %s",
                    resource.hash_type,
                    resource.hash,
                )
                lines += ["echo '%(md5sum)s  %(destination)s' | md5sum -c -"]
                lines += ["status=$?"]
                lines += ["if [ $status -ne 0 ]; then rm -fv %(destination)s; exit $status; fi"]
                values['md5sum'] = resource.hash
            else:
                job.logger.warning(
                    "Don't know how to process hash type %s, ignoring hash",
                    resource.hash_type,
                )

        job.command = "\n".join(lines) + "\n"
        job.command = job.command % values
        self.execute(job)

        if job.status != eva.job.COMPLETE:
            raise eva.exceptions.RetryException("Download of '%s' failed." % resource.url)

        if not self.post_to_productstatus:
            return

        job.logger.info('Creating a new DataInstance on the Productstatus server...')
        datainstance = self.api.datainstance.create()
        datainstance.data = resource.data
        datainstance.format = resource.format
        datainstance.expires = datetime.datetime.now() + self.lifetime
        datainstance.servicebackend = self.api.servicebackend[self.env['EVA_OUTPUT_SERVICE_BACKEND_UUID']]
        datainstance.url = os.path.join(self.env['EVA_OUTPUT_BASE_URL'], filename)
        eva.retry_n(datainstance.save,
                    exceptions=(productstatus.exceptions.ServiceUnavailableException,),
                    give_up=0)
        job.logger.info('DataInstance %s, expires %s', datainstance, datainstance.expires)
        job.logger.info('The file %s has been successfully copied to %s', resource.url, datainstance.url)
