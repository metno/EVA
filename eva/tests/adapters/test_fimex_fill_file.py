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
    adapter_class = eva.adapter.FimexFillFileAdapter
    environment = {
        'EVA_FIMEX_FILL_FILE_TEMPLATE': '/template/{{reference_time|iso8601_compact}}.nc',
        'EVA_INPUT_DATA_FORMAT': 'foo',
        'EVA_INPUT_PRODUCT': 'foo',
        'EVA_INPUT_SERVICE_BACKEND': 'foo',
        'EVA_OUTPUT_BASE_URL': 'file:///foo',
        'EVA_OUTPUT_DATA_FORMAT': 'netcdf',
        'EVA_OUTPUT_FILENAME_PATTERN': '/output/{{reference_time|iso8601_compact}}.nc',
        'EVA_OUTPUT_PRODUCT': 'foo',
        'EVA_OUTPUT_SERVICE_BACKEND': 'foo',
        'EVA_PRODUCTSTATUS_API_KEY': 'foo',
        'EVA_PRODUCTSTATUS_USERNAME': 'foo',
    }

    def test_with_partial(self):
        """!
        @brief Test that the adapter requires EVA_INPUT_PARTIAL=NO.
        """
        self.env['EVA_INPUT_PARTIAL'] = 'YES'
        with self.assertRaises(eva.exceptions.InvalidConfigurationException):
            self.create_adapter()

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
        print(job.command)
        self.assertTrue("[ ! -f '/output/20160101T181500Z.nc' ] && cp -v '/template/20160101T181500Z.nc' '/output/20160101T181500Z.nc'" in job.command)
        self.assertTrue("fimex --input.file '/foo/bar/baz' --output.fillFile '/output/20160101T181500Z.nc'" in job.command)

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
        self.assertEqual(len(resources['productinstance']), 0)
        self.assertEqual(len(resources['data']), 0)
        self.assertEqual(len(resources['datainstance']), 1)
        self.assertEqual(resources['datainstance'][0].servicebackend, self.adapter.output_service_backend)
        self.assertEqual(resources['datainstance'][0].partial, True)
