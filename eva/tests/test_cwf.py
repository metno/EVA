import unittest
import datetime
import mock
import logging

import productstatus
import productstatus.api

import eva.executor
import eva.adapter.cwf


class TestCWFAdapter(unittest.TestCase):
    def setUp(self):
        self.env = {
            'EVA_CWF_DOMAIN': "NRPA_EUROPE_0_1",
            'EVA_CWF_INPUT_MIN_DAYS': "4",
            'EVA_CWF_MODULES': "ecdis4cwf/1.1.0",
            'EVA_CWF_OUTPUT_DAYS': "3",
            'EVA_CWF_OUTPUT_DIRECTORY_PATTERN': "/tmp/{{reference_time|iso8601_compact}}",
            'EVA_CWF_SCRIPT_PATH': "ecdis4cwf.sh",
            'EVA_INPUT_DATA_FORMAT': "nil",
            'EVA_INPUT_PRODUCT': "nil",
            'EVA_INPUT_SERVICE_BACKEND': "nil",
        }
        self.productstatus_api = productstatus.api.Api('http://localhost:8000')
        self.logger = logging.getLogger('root')
        self.zookeeper = None
        self.executor = eva.executor.NullExecutor(None, self.env, self.logger, self.zookeeper)

    def create_adapter(self):
        self.adapter = eva.adapter.CWFAdapter(self.env, self.executor, self.productstatus_api, self.logger, self.zookeeper)

    def test_parse_file_recognition_output(self):
        stdout = ['/tmp/meteo20160606_00.nc  time = "2016-06-06 12" ;',
                  '/tmp/meteo20160606_01.nc  time = "2016-06-08" ;']
        self.create_adapter()
        output = self.adapter.parse_file_recognition_output(stdout)
        self.assertEqual(len(output), 2)
        self.assertEqual(output[0]['path'], '/tmp/meteo20160606_00.nc')
        self.assertEqual(output[1]['path'], '/tmp/meteo20160606_01.nc')
        timestamps = [
            eva.coerce_to_utc(datetime.datetime(2016, 6, 6, 12)),
            eva.coerce_to_utc(datetime.datetime(2016, 6, 8)),
        ]
        self.assertListEqual(output[0]['time_steps'], [timestamps[0]])
        self.assertListEqual(output[1]['time_steps'], [timestamps[1]])

    def test_generate_resources(self):
        self.create_adapter()
        self.adapter.api = mock.MagicMock()

        self.adapter.output_product = mock.MagicMock()
        self.adapter.output_data_format = mock.MagicMock()
        self.adapter.output_service_backend = mock.MagicMock()

        job = eva.job.Job('foo', self.logger)
        job.stdout = ['/tmp/meteo20160606_00.nc  time = "2016-06-06 12", "2016-06-06 15", "2016-06-06 18", "2016-06-06 21", "2016-06-07" ;']
        job.output_files = self.adapter.parse_file_recognition_output(job.stdout)

        resource = mock.MagicMock()
        resource.data.productinstance.reference_time = eva.coerce_to_utc(datetime.datetime(2016, 6, 6, 12))

        resources = self.adapter.generate_resources(resource, job)
        self.assertEqual(resources['productinstance'][0].product, self.adapter.output_product)
        self.assertEqual(resources['datainstance'][0].url, 'file:///tmp/meteo20160606_00.nc')
        self.assertEqual(resources['data'][0].time_period_begin,
                         eva.coerce_to_utc(datetime.datetime(2016, 6, 6, 12)))
        self.assertEqual(resources['data'][0].time_period_end,
                         eva.coerce_to_utc(datetime.datetime(2016, 6, 7)))

    def test_post_resources(self):
        resources = {
            'productinstance': [mock.MagicMock()],
            'data': [mock.MagicMock()],
            'datainstance': [mock.MagicMock()],
        }
        self.create_adapter()
        self.adapter.api = mock.MagicMock()
        self.adapter.post_resources(resources)
        for key, r in resources.items():
            r[0].save.assert_called_with()
