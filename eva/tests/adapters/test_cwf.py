import eva.tests
import eva.tests.schemas
import eva.adapter
import eva.exceptions
import eva.job

from unittest import mock
import httmock
import datetime

import eva.adapter.cwf


class TestCWFAdapter(eva.tests.BaseTestAdapter):
    adapter_class = eva.adapter.CWFAdapter
    config_ini = \
"""
[adapter]
cwf_domain = NRPA_EUROPE_0_1
cwf_input_min_days = 4
cwf_modules = ecdis4cwf/1.1.0
cwf_output_days = 3
cwf_output_directory_pattern = /tmp/{{reference_time|iso8601_compact}}
cwf_script_path = ecdis4cwf.sh
input_data_format = nil
input_product = nil
input_service_backend = nil
"""  # NOQA

    def setUp(self):
        super().setUp()
        self.setup_productstatus()

    def test_init(self):
        with httmock.HTTMock(*eva.tests.schemas.SCHEMAS):
            return super().test_init()

    def test_parse_file_recognition_output(self):
        stdout = ['/tmp/meteo20160606_00.nc  time = "2016-06-06 12" ;',
                  '/tmp/meteo20160606_01.nc  time = "2016-06-08" ;',
                  '/tmp/meteo20160606_00.nml',
                  ]
        with httmock.HTTMock(*eva.tests.schemas.SCHEMAS):
            self.create_adapter()
        output = self.adapter.parse_file_recognition_output(stdout)
        self.assertEqual(len(output), 3)
        self.assertEqual(output[0]['path'], '/tmp/meteo20160606_00.nc')
        self.assertEqual(output[1]['path'], '/tmp/meteo20160606_01.nc')
        self.assertEqual(output[2]['path'], '/tmp/meteo20160606_00.nml')
        timestamps = [
            eva.coerce_to_utc(datetime.datetime(2016, 6, 6, 12)),
            eva.coerce_to_utc(datetime.datetime(2016, 6, 8)),
        ]
        self.assertListEqual(output[0]['time_steps'], [timestamps[0]])
        self.assertListEqual(output[1]['time_steps'], [timestamps[1]])
        self.assertEqual(output[0]['extension'], '.nc')
        self.assertEqual(output[1]['extension'], '.nc')
        self.assertEqual(output[2]['extension'], '.nml')

    def test_generate_resources(self):
        with httmock.HTTMock(*eva.tests.schemas.SCHEMAS):
            self.create_adapter()

        self.adapter.output_product = mock.MagicMock()
        self.adapter.output_data_format = mock.MagicMock()
        self.adapter.output_service_backend = mock.MagicMock()
        self.adapter.nml_data_format = mock.MagicMock()

        resource = mock.MagicMock()
        job = self.create_job(resource)
        job.stdout = [
            '/tmp/meteo20160606_00.nc  time = "2016-06-06 12", "2016-06-06 15", "2016-06-06 18", "2016-06-06 21", "2016-06-07" ;',
            '/tmp/meteo20160606_00.nml',
        ]
        job.output_files = self.adapter.parse_file_recognition_output(job.stdout)
        job.resource.data.productinstance.reference_time = eva.coerce_to_utc(datetime.datetime(2016, 6, 6, 12))

        job.set_status(eva.job.COMPLETE)
        self.adapter.finish_job(job)

        with httmock.HTTMock(*eva.tests.schemas.SCHEMAS):
            resources = self.generate_resources(job)

        self.assertEqual(resources['productinstance'][0].args[0]['product'], self.adapter.output_product)
        self.assertEqual(resources['datainstance'][0].url, 'file:///tmp/meteo20160606_00.nc')
        self.assertEqual(resources['datainstance'][0].format, self.adapter.output_data_format)
        self.assertEqual(resources['data'][0].args[0]['time_period_begin'],
                         eva.coerce_to_utc(datetime.datetime(2016, 6, 6, 12)))
        self.assertEqual(resources['data'][0].args[0]['time_period_end'],
                         eva.coerce_to_utc(datetime.datetime(2016, 6, 7)))

        self.assertEqual(resources['datainstance'][1].url, 'file:///tmp/meteo20160606_00.nml')
        self.assertEqual(resources['datainstance'][1].format, self.adapter.nml_data_format)
        self.assertEqual(resources['data'][1].args[0]['time_period_begin'], None)
        self.assertEqual(resources['data'][1].args[0]['time_period_end'], None)

        self.assertEqual(len(resources['productinstance']), 1)
        self.assertEqual(len(resources['data']), 2)
        self.assertEqual(len(resources['datainstance']), 2)
