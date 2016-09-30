import eva.tests
import eva.adapter
import eva.exceptions
import eva.job

import mock


class TestChecksumVerificationAdapter(eva.tests.BaseTestAdapter):
    adapter_class = eva.adapter.ChecksumVerificationAdapter
    environment = {
        'EVA_INPUT_SERVICE_BACKEND': 'foo',
        'EVA_INPUT_WITH_HASH': 'NO',
        'EVA_PRODUCTSTATUS_API_KEY': 'foo',
        'EVA_PRODUCTSTATUS_USERNAME': 'foo',
    }

    def test_with_hash(self):
        """!
        @brief Test that the adapter requires EVA_INPUT_WITH_HASH=NO.
        """
        self.env['EVA_INPUT_WITH_HASH'] = 'YES'
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
        self.adapter.finish_job(job)

    def test_generate_resources(self):
        """!
        @brief Test that the adapter skips failed jobs.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        job = self.create_job(resource)
        job.set_status(eva.job.COMPLETE)
        self.adapter.finish_job(job)
        md5sum = '401b30e3b8b5d629635a5c613cdb7919'
        job.stdout = md5sum
        resources = self.generate_resources(job)
        self.assertEqual(resources['datainstance'][0].hash_type, str('md5'))
        self.assertEqual(resources['datainstance'][0].hash, md5sum)
        self.assertEqual(len(resources['productinstance']), 0)
        self.assertEqual(len(resources['data']), 0)
        self.assertEqual(len(resources['datainstance']), 1)
