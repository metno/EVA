import eva
import eva.tests
import eva.tests.schemas
import eva.adapter
import eva.exceptions
import eva.job

import mock


class TestDistributionAdapter(eva.tests.BaseTestAdapter):
    adapter_class = eva.adapter.DistributionAdapter
    config_ini = \
"""
[adapter]
input_service_backend = foo
output_base_url = file:///foo
"""  # NOQA

    def test_create_adapter_recursive(self):
        """!
        @brief Test that the adapter will not start if configured to run in a recursive loop.
        """
        self.config['adapter']['output_service_backend'] = 'foo'
        with self.assertRaises(eva.exceptions.InvalidConfigurationException):
            self.create_adapter()

    def test_create_job(self):
        """!
        @brief Test that job creation generates the correct command line.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        resource.url = 'file:///foo/bar/baz'
        job = self.create_job(resource)
        command_line_fragment = "cp --verbose /foo/bar/baz /foo/baz\n"
        self.assertTrue(command_line_fragment in job.command)

    def test_finish_job_and_generate_resources(self):
        """!
        @brief Test that job finish works and doesn't throw any exceptions.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        resource.url = 'file:///foo/bar/baz'
        job = self.create_job(resource)
        job.service_backend = 'foo'
        job.set_status(eva.job.COMPLETE)
        self.adapter.finish_job(job)
        resources = self.generate_resources(job)
        self.assertEqual(len(resources['productinstance']), 0)
        self.assertEqual(len(resources['data']), 0)
        self.assertEqual(len(resources['datainstance']), 1)
        self.assertEqual(resources['datainstance'][0].url, 'file:///foo/baz')
