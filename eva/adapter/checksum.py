import eva
import eva.base.adapter
import eva.job
import eva.exceptions


class ChecksumVerificationAdapter(eva.base.adapter.BaseAdapter):
    """!
    An adapter that verifies checksums on data sets according to auxiliary files.

    At the moment, only md5sum is supported, and is expected to be in the same
    directory as the data sets, with the suffix ".md5".

    If the checksum is correct, the .md5 file is deleted, and the hash is added
    to the Productstatus database. If the checksum fails, the file is left
    intact, and the task fails and is NOT restarted.
    """

    REQUIRED_CONFIG = [
        'input_service_backend',
    ]

    OPTIONAL_CONFIG = [
        'input_data_format',
        'input_product',
        'input_with_hash',
    ]

    def adapter_init(self):
        """!
        @brief This adapter requires Productstatus write access to be of any use.
        """
        if self.env['input_with_hash'] is not False:
            raise eva.exceptions.InvalidConfigurationException(
                'This adapter MUST be configured with input_with_hash=NO in order to avoid recursive loops.'
            )

    def create_job(self, job):
        """!
        @brief Return a Job object that will check the file's md5sum against a
        stored hash in a corresponding file.
        """
        job.dataset_filename = eva.url_to_filename(job.resource.url)
        job.md5_filename = job.dataset_filename + '.md5'
        job.logger.info("Starting verification of file '%s' against md5sum file '%s'.", job.dataset_filename, job.md5_filename)

        lines = [
            '#!/bin/bash',
            '#$ -S /bin/bash',  # for GridEngine compatibility
            'set -e',
            'cat %(md5_filename)s',  # for hash detection in generate_resources()
            'printf "%%s  %(dataset_filename)s\\n" $(cat %(md5_filename)s) | md5sum --check --status --strict -',
        ]
        values = {
            'dataset_filename': job.dataset_filename,
            'md5_filename': job.md5_filename,
        }

        job.command = "\n".join(lines) + "\n"
        job.command = job.command % values

    def finish_job(self, job):
        if not job.complete():
            job.logger.error("md5sum checking of '%s' failed, skipping further processing!", job.resource.url)
            self.statsd.incr('eva_md5sum_fail')
            return
        job.resource_hash_type = str('md5')
        job.resource_hash = ''.join(job.stdout).strip()
        if len(job.resource_hash) != 32:
            raise eva.exceptions.RetryException('md5sum hash (%s) length does not equal 32, this must be a bug in the job output?' % job.resource_hash)
        job.logger.info('DataInstance %s has md5sum hash %s.', job.resource, job.resource_hash)

    def generate_resources(self, job, resources):
        """!
        @brief Generate a set of Productstatus resources based on job output.

        This adapter will modify the original DataInstance object so that it
        contains an MD5 hash.
        """
        job.resource.hash_type = job.resource_hash_type
        job.resource.hash = job.resource_hash

        resources['datainstance'] += [job.resource]
