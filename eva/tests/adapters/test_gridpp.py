import eva
import eva.tests
import eva.tests.schemas
import eva.adapter
import eva.exceptions
import eva.job

import mock
import httmock
import datetime


class TestGridPPAdapter(eva.tests.BaseTestAdapter):
    adapter_class = eva.adapter.GridPPAdapter
    config_ini = \
"""
[adapter]
gridpp_generic_options = --reftime {{reference_time|iso8601_compact}}
gridpp_input_options = inopts
gridpp_output_options = outopts
gridpp_threads = 4
input_data_format = foo
input_product = foo
input_service_backend = foo
output_filename_pattern = /out/{{reference_time|iso8601_compact}}
output_data_format = foo
output_product = foo
output_service_backend = foo
"""  # NOQA

    def test_create_job(self):
        """!
        @brief Test that job creation generates the correct GridPP command line.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        resource.url = 'file:///foo/bar/baz'
        resource.data.productinstance.reference_time = eva.coerce_to_utc(datetime.datetime(2016, 1, 1, 18, 15, 0))
        with httmock.HTTMock(*eva.tests.schemas.SCHEMAS):
            job = self.create_job(resource)
        self.assertTrue('cp -v /foo/bar/baz /out/20160101T181500Z' in job.command)
        self.assertTrue('export OMP_NUM_THREADS=4' in job.command)
        self.assertTrue('gridpp /foo/bar/baz inopts /out/20160101T181500Z outopts --reftime 20160101T181500Z' in job.command)

    def test_finish_job_and_generate_resources(self):
        """!
        @brief Test that job finish works and doesn't throw any exceptions.
        """
        self.create_adapter()
        resource = mock.MagicMock()
        resource.url = 'file:///foo/bar/baz'
        resource.data.productinstance.reference_time = eva.coerce_to_utc(datetime.datetime(2016, 1, 1, 18, 15, 0))
        with httmock.HTTMock(*eva.tests.schemas.SCHEMAS):
            job = self.create_job(resource)
            job.set_status(eva.job.COMPLETE)
            self.adapter.finish_job(job)
            resources = self.generate_resources(job)
        self.assertEqual(len(resources['productinstance']), 1)
        self.assertEqual(len(resources['data']), 1)
        self.assertEqual(len(resources['datainstance']), 1)
        self.assertEqual(resources['datainstance'][0].url, 'file:///out/20160101T181500Z')
