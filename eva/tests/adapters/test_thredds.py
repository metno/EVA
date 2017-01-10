import eva.tests
import eva.tests.schemas
import eva.adapter
import eva.exceptions
import eva.job

from unittest import mock


class TestThreddsAdapter(eva.tests.BaseTestAdapter):
    adapter_class = eva.adapter.ThreddsAdapter
    config_ini = \
"""
[adapter]
input_data_format = foo
input_product = foo
input_service_backend = foo
output_service_backend = foo
thredds_base_url = http://bar/baz
"""  # NOQA

    def test_create_job(self):
        """!
        @brief Test that the job is created correctly.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        resource.url = 'file:///path/to/foo.bar'
        job = self.create_job(resource)
        self.assertEqual(job.thredds_url, 'http://bar/baz/foo.bar')
        self.assertEqual(job.thredds_html_url, 'http://bar/baz/foo.bar.html')

    def test_finish_job_exception(self):
        """
        Test that finishing the job raises an exception if it fails.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        resource.url = 'file:///path/to/foo.bar'
        job = self.create_job(resource)
        job.set_status(eva.job.FAILED)
        with self.assertRaises(eva.exceptions.RetryException):
            self.adapter.finish_job(job)

    def test_generate_resources(self):
        """!
        @brief Test that the adapter generates correct resources for the job output.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        resource.url = 'file:///path/to/foo.bar'
        job = self.create_job(resource)
        job.set_status(eva.job.COMPLETE)
        resources = self.adapter.default_resource_dictionary()
        self.adapter.generate_resources(job, resources)
        self.assertEqual(len(resources['productinstance']), 0)
        self.assertEqual(len(resources['data']), 0)
        self.assertEqual(len(resources['datainstance']), 1)
        self.assertEqual(resources['datainstance'][0].args[0]['url'], 'http://bar/baz/foo.bar')
