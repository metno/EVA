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
    config_ini = \
"""
[adapter]
fimex_fill_file_ncfill_path = /path/to/ncfill
fimex_fill_file_template_directory = /template
input_data_format = foo
input_product = foo
input_service_backend = foo
output_base_url = file:///foo
output_data_format = netcdf
output_filename_pattern = /output/{{reference_time|iso8601_compact}}.nc
output_product = foo
output_service_backend = foo
"""  # NOQA

    def test_with_partial(self):
        """!
        @brief Test that the adapter requires input_partial=NO.
        """
        self.config['adapter']['input_partial'] = 'YES'
        with self.assertRaises(eva.exceptions.InvalidConfigurationException):
            self.create_adapter()

    def test_create_job(self):
        """!
        @brief Test that job creation generates the correct FIMEX command line.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        resource.url = 'file:///foo/bar/baz'
        resource.format.slug = 'netcdf'
        resource.data.productinstance.reference_time = eva.coerce_to_utc(datetime.datetime(2016, 1, 1, 18, 15, 0))
        with httmock.HTTMock(*eva.tests.schemas.SCHEMAS):
            job = self.create_job(resource)
        check_command = ' '.join([
            "time",
            "/path/to/ncfill",
            "--input '/foo/bar/baz'",
            "--output '/output/20160101T181500Z.nc'",
            "--input_format 'netcdf'",
            "--reference_time '2016-01-01T18:15:00+0000'",
            "--template_directory '/template'"
        ])
        self.assertTrue(check_command in job.command)

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
