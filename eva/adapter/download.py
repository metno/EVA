import os
import re

import eva
import eva.base.adapter
import eva.job
import eva.exceptions


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

    PRODUCTSTATUS_REQUIRED_CONFIG = [
        'EVA_OUTPUT_BASE_URL',
        'EVA_OUTPUT_SERVICE_BACKEND',
    ]

    def init(self):
        """!
        @brief Check that optional configuration is consistent.
        """
        self.check_hash = self.env['EVA_DOWNLOAD_CHECK_HASH']
        if self.env['EVA_OUTPUT_SERVICE_BACKEND'] in self.env['EVA_INPUT_SERVICE_BACKEND']:
            raise eva.exceptions.InvalidConfigurationException('EVA_OUTPUT_SERVICE_BACKEND cannot be present in the list of EVA_INPUT_SERVICE_BACKEND, as that will result in an endless loop.')

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
            self.service_backend = self.api.servicebackend[self.env['EVA_OUTPUT_SERVICE_BACKEND']]
        else:
            self.service_backend = None

        bytes_sec = self.parse_bytes_sec_from_lines(job.stderr)
        if bytes_sec is not None:
            if self.service_backend is not None:
                self.statsd.gauge('download_rate', bytes_sec, {'service_backend': self.service_backend.slug})
            else:
                self.statsd.gauge('download_rate', bytes_sec)
            job.logger.info('Download speed is %d bytes/sec', bytes_sec)

    def generate_resources(self, job, resources):
        """!
        @brief Generate a set of Productstatus resources based on job output.

        This adapter will post a new DataInstance using the same Data and
        ProductInstance as the input resource.
        """
        datainstance = self.api.datainstance.create()
        datainstance.data = job.resource.data
        datainstance.format = job.resource.format
        datainstance.expires = self.expiry_from_lifetime()
        datainstance.servicebackend = self.service_backend
        datainstance.url = os.path.join(self.env['EVA_OUTPUT_BASE_URL'], job.base_filename)
        resources['datainstance'] += [datainstance]
