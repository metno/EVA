import os
import re

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
        'EVA_DOWNLOAD_DESTINATION': {
            'type': 'string',
            'help': 'Where to place downloaded files',
            'default': ''
        },
        'EVA_DOWNLOAD_CHECK_HASH': {
            'type': 'bool',
            'help': 'Whether or not to check the hash of downloaded files against the Productstatus hash.',
            'default': 'YES',
        },
    }

    REQUIRED_CONFIG = [
        'EVA_DOWNLOAD_DESTINATION',
        'EVA_INPUT_SERVICE_BACKEND',
    ]

    OPTIONAL_CONFIG = [
        'EVA_DOWNLOAD_CHECK_HASH',
        'EVA_INPUT_DATA_FORMAT',
        'EVA_INPUT_PARTIAL',
        'EVA_INPUT_PRODUCT',
        'EVA_OUTPUT_BASE_URL',
        'EVA_OUTPUT_LIFETIME',
        'EVA_OUTPUT_SERVICE_BACKEND',
    ]

    def init(self):
        """!
        @brief Check that optional configuration is consistent.
        """
        self.check_hash = self.env['EVA_DOWNLOAD_CHECK_HASH']
        if self.has_valid_output_config():
            self.post_to_productstatus = True
            self.require_productstatus_credentials()
            if self.env['EVA_OUTPUT_SERVICE_BACKEND'] in self.env['EVA_INPUT_SERVICE_BACKEND']:
                raise eva.exceptions.InvalidConfigurationException('EVA_OUTPUT_SERVICE_BACKEND cannot be present in the list of EVA_INPUT_SERVICE_BACKEND, as that will result in an endless loop.')
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
            (self.env['EVA_OUTPUT_SERVICE_BACKEND'] is not None)
        )

    def parse_bytes_sec_from_lines(self, lines):
        """!
        @brief Return the number of bytes per second from a list of wget output lines.
        """
        # 100  285M  100  285M    0     0   431M      0 --:--:-- --:--:-- --:--:--  431M
        rate_regex = re.compile('^\d+\s+\w+\s+\d+\s+\w+\s+\d+\s+\d+\s+(\d+)([A-Z]).+$')
        for line in lines:
            line = line.split('\r')[-1]
            matches = rate_regex.match(line)
            if matches:
                return eva.convert_to_bytes(matches.group(1), matches.group(2))
        return None

    def create_job(self, message_id, resource):
        """!
        @brief Download a file, and optionally post the result to Productstatus.
        """
        job = eva.job.Job(message_id, self.logger)
        job.base_filename = os.path.basename(resource.url)

        lines = [
            "#!/bin/bash",
            "#$ -S /bin/bash",  # for GridEngine compatibility
            "curl --fail --output '%(destination)s' %(url)s",
        ]
        values = {
            'url': resource.url,
            'destination': os.path.join(self.env['EVA_DOWNLOAD_DESTINATION'], job.base_filename),
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
        return job

    def finish_job(self, job):
        if not job.complete():
            raise eva.exceptions.RetryException("Download of '%s' failed." % job.resource.url)

        if self.env['EVA_OUTPUT_SERVICE_BACKEND']:
            service_backend = self.api.servicebackend[self.env['EVA_OUTPUT_SERVICE_BACKEND']]
        else:
            service_backend = None

        bytes_sec = self.parse_bytes_sec_from_lines(job.stderr)
        if bytes_sec is not None:
            if service_backend is not None:
                self.statsd.gauge('download_rate', bytes_sec, {'service_backend': service_backend.slug})
            else:
                self.statsd.gauge('download_rate', bytes_sec)
            job.logger.info('Download speed is %d bytes/sec', bytes_sec)

        if not self.post_to_productstatus:
            return

        job.logger.info('Creating a new DataInstance on the Productstatus server...')
        datainstance = self.api.datainstance.create()
        datainstance.data = job.resource.data
        datainstance.format = job.resource.format
        datainstance.expires = self.expiry_from_lifetime()
        datainstance.servicebackend = service_backend
        datainstance.url = os.path.join(self.env['EVA_OUTPUT_BASE_URL'], job.base_filename)
        eva.retry_n(datainstance.save,
                    exceptions=(productstatus.exceptions.ServiceUnavailableException,),
                    give_up=0)
        job.logger.info('DataInstance %s, expires %s', datainstance, datainstance.expires)
        job.logger.info('The file %s has been successfully copied to %s', job.resource.url, datainstance.url)
