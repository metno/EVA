from unittest import mock

import eva.tests
import eva.adapter
import eva.adapter.checksum
import eva.exceptions
import eva.job


class TestChecksumVerificationAdapter(eva.tests.BaseTestAdapter):
    adapter_class = eva.adapter.ChecksumVerificationAdapter
    config_ini = \
"""
[adapter]
input_service_backend = foo
input_with_hash = NO
"""  # NOQA

    def test_with_hash(self):
        """!
        @brief Test that the adapter requires input_with_hash=NO.
        """
        self.config['adapter']['input_with_hash'] = 'YES'
        with self.assertRaises(eva.exceptions.InvalidConfigurationException):
            self.create_adapter()

    def test_create_job(self):
        """!
        @brief Test that job creation works and doesn't throw any exceptions.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        self.create_job(resource)

    def test_finish_job_failed(self):
        """!
        @brief Test that the adapter skips failed jobs.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        job = self.create_job(resource)
        job.set_status(eva.job.FAILED)
        with self.assertRaises(eva.exceptions.RetryException):
            self.adapter.finish_job(job)

    def test_generate_resources(self):
        """!
        @brief Test that the adapter skips failed jobs.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        job = self.create_job(resource)
        job.set_status(eva.job.COMPLETE)
        md5sum = '401b30e3b8b5d629635a5c613cdb7919'
        job.stdout = ["eva.adapter.checksum.md5 " + md5sum]
        self.adapter.finish_job(job)
        resources = self.generate_resources(job)
        self.assertEqual(resources['datainstance'][0].hash_type, str('md5'))
        self.assertEqual(resources['datainstance'][0].hash, md5sum)
        self.assertEqual(len(resources['productinstance']), 0)
        self.assertEqual(len(resources['data']), 0)
        self.assertEqual(len(resources['datainstance']), 1)

    def test_find_md5sum(self):
        check = '401b30e3b8b5d629635a5c613cdb7919'
        stdout = [
            "foo bar",
            "eva.adapter.checksum.md5 " + check,
            "baz",
        ]
        md5 = eva.adapter.checksum.job_output_md5sum(stdout)
        self.assertEqual(md5, check)

    def test_find_md5sum_errfmt(self):
        stdout = [
            "eva.adapter.checksum.md5 test",
        ]
        md5 = eva.adapter.checksum.job_output_md5sum(stdout)
        self.assertIsNone(md5)
