import eva
import eva.tests
import eva.tests.schemas
import eva.adapter
import eva.exceptions
import eva.job

import mock


class TestDistributionAdapter(eva.tests.BaseTestAdapter):
    adapter_class = eva.adapter.DistributionAdapter
    environment = {
        'EVA_INPUT_SERVICE_BACKEND': 'foo',
        'EVA_OUTPUT_BASE_URL': 'file:///foo',
        'EVA_OUTPUT_SERVICE_BACKEND': 'bar',
        'EVA_PRODUCTSTATUS_API_KEY': 'foo',
        'EVA_PRODUCTSTATUS_USERNAME': 'foo',
    }

    def test_create_adapter_recursive(self):
        """!
        @brief Test that the adapter will not start if configured to run in a recursive loop.
        """
        self.env['EVA_OUTPUT_SERVICE_BACKEND'] = 'foo'
        with self.assertRaises(eva.exceptions.InvalidConfigurationException):
            self.create_adapter()

    def test_create_job(self):
        """!
        @brief Test that job creation generates the correct command line.
        """
        del self.env['EVA_PRODUCTSTATUS_API_KEY']
        self.create_adapter()
        resource = mock.MagicMock()
        resource.url = 'file:///foo/bar/baz'
        self.adapter.api = mock.MagicMock()
        job = self.create_job(resource)
        command_line_fragment = "cp --verbose /foo/bar/baz /foo/baz\n"
        self.assertTrue(command_line_fragment in job.command)

    def test_finish_job_and_generate_resources(self):
        """!
        @brief Test that job finish works and doesn't throw any exceptions.
        """
        del self.env['EVA_PRODUCTSTATUS_API_KEY']
        self.create_adapter()
        resource = mock.MagicMock()
        resource.url = 'file:///foo/bar/baz'
        self.adapter.api = mock.MagicMock()
        job = self.create_job(resource)
        job.service_backend = 'foo'
        job.set_status(eva.job.COMPLETE)
        self.adapter.finish_job(job)
        resources = self.generate_resources(job)
        self.assertEqual(len(resources['productinstance']), 0)
        self.assertEqual(len(resources['data']), 0)
        self.assertEqual(len(resources['datainstance']), 1)
        self.assertEqual(resources['datainstance'][0].url, 'file:///foo/baz')
