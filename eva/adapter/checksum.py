import eva
import eva.base.adapter
import eva.job
import eva.exceptions


def job_output_md5sum(stdout):
    """
    Find the MD5 sum from the job output.
    """
    for line in stdout:
        if not line.startswith('eva.adapter.checksum.md5 '):
            continue
        tokens = line.strip().split()
        md5 = tokens[1]
        if len(md5) != 32:
            break
        return md5
    return None


class ChecksumVerificationAdapter(eva.base.adapter.BaseAdapter):
    """
    The ChecksumVerificationAdapter verifies checksums on data sets, according
    to auxiliary files that have the similar name. For instance, it will
    calculate the checksum on the file ``foo`` if a ``foo.md5`` file exists in
    the same directory.

    If the checksum is correct, it is added to the Productstatus resource under
    the fields ``hash`` and ``hash_type``. If the checksum fails, the task
    fails, and is NOT requeued.

    At the moment, only MD5 checksums are supported.
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

        values = {
            'dataset_filename': job.dataset_filename,
            'md5_filename': job.md5_filename,
        }

        job.command = [
            'set -e',
            'echo -n "eva.adapter.checksum.md5 "',
            'cat %(md5_filename)s' % values,  # for hash detection in generate_resources()
            'printf "%%s  %(dataset_filename)s\\n" $(cat %(md5_filename)s) | md5sum --check --status --strict -' % values,
        ]

    def finish_job(self, job):
        if not job.complete():
            self.statsd.incr('eva_md5sum_fail')
            raise eva.exceptions.RetryException("md5sum checking of '%s' failed, skipping further processing!" % job.resource.url)
        job.resource_hash_type = str('md5')
        job.resource_hash = job_output_md5sum(job.stdout)
        if job.resource_hash is None:
            raise eva.exceptions.RetryException('md5sum hash %s length does not equal 32, this must be a bug in the job output?' % job.resource_hash)
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
