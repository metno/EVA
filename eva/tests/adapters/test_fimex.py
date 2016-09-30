import eva
import eva.tests
import eva.tests.schemas
import eva.adapter
import eva.exceptions
import eva.job

import mock
import httmock
import datetime


class TestFimexAdapter(eva.tests.BaseTestAdapter):
    adapter_class = eva.adapter.FimexAdapter
    environment = {
        'EVA_FIMEX_PARAMETERS': '{{reference_time|iso8601_compact}} {{datainstance.url}} {{input_filename}}',
        'EVA_INPUT_DATA_FORMAT': 'foo',
        'EVA_INPUT_PRODUCT': 'foo',
        'EVA_INPUT_SERVICE_BACKEND': 'foo',
        'EVA_OUTPUT_BASE_URL': 'file:///foo',
        'EVA_OUTPUT_DATA_FORMAT': 'netcdf',
        'EVA_OUTPUT_FILENAME_PATTERN': '/foo/bar.nc',
        'EVA_OUTPUT_PRODUCT': 'foo',
        'EVA_OUTPUT_SERVICE_BACKEND': 'foo',
        'EVA_PRODUCTSTATUS_API_KEY': 'foo',
        'EVA_PRODUCTSTATUS_USERNAME': 'foo',
    }

    def test_create_job(self):
        """!
        @brief Test that job creation generates the correct FIMEX command line.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        resource.url = 'file:///foo/bar/baz'
        resource.data.productinstance.reference_time = eva.coerce_to_utc(datetime.datetime(2016, 1, 1, 18, 15, 0))
        with httmock.HTTMock(*eva.tests.schemas.SCHEMAS):
            job = self.create_job(resource)
        command_line_fragment = "fimex --input.file '/foo/bar/baz' --output.file '/foo/bar.nc' 20160101T181500Z file:///foo/bar/baz baz\n"
        self.assertTrue(command_line_fragment in job.command)

    def test_finish_job_and_generate_resources(self):
        """!
        @brief Test that job finish works and doesn't throw any exceptions.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        with httmock.HTTMock(*eva.tests.schemas.SCHEMAS):
            job = self.create_job(resource)
        job.set_status(eva.job.COMPLETE)
        self.adapter.finish_job(job)
        resources = self.generate_resources(job)
        self.assertEqual(len(resources['productinstance']), 1)
        self.assertEqual(len(resources['data']), 1)
        self.assertEqual(len(resources['datainstance']), 1)
        self.assertEqual(resources['datainstance'][0].args[0]['servicebackend'], self.adapter.output_service_backend)
