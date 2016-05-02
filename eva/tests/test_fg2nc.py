import unittest
import datetime
import dateutil.tz
import mock
import uuid
import re
import logging

import productstatus
import productstatus.api

import eva.executor
import eva.adapter.fimex_grib_to_netcdf


BLANK_UUID = '00000000-0000-0000-0000-000000000000'


class TestFimexGRIB2NetCDFAdapter(unittest.TestCase):

    def setUp(self):
        self.env = {
            'EVA_FG2NC_LIB': '/eva-adapter-support',
            'EVA_FG2NC_TEMPLATEDIR': '/template',
            'EVA_INPUT_DATA_FORMAT': BLANK_UUID,
            'EVA_INPUT_PRODUCT': BLANK_UUID,
            'EVA_INPUT_SERVICE_BACKEND': BLANK_UUID,
            'EVA_OUTPUT_BASE_URL': '/output',
            'EVA_OUTPUT_FILENAME_PATTERN': '/output/%Y%m%dT%H%M%S',
            'EVA_OUTPUT_PRODUCT': BLANK_UUID,
            'EVA_OUTPUT_SERVICE_BACKEND': BLANK_UUID,
        }
        self.productstatus_api = productstatus.api.Api('http://localhost:8000')
        self.logger = logging.getLogger('root')
        self.zookeeper = None
        self.executor = eva.executor.NullExecutor(None, self.env, self.logger, self.zookeeper)

    def create_adapter(self):
        self.adapter = eva.adapter.FimexGRIB2NetCDFAdapter(self.env, self.executor, self.productstatus_api, self.logger, self.zookeeper)

    def test_productstatus_read_only_default(self):
        """
        @brief Test that DownloadAdapter doesn't post to Productstatus when
        output configuration is not given.
        """
        compare_command = '#!/bin/bash # -S /bin/bash /eva-adapter-support/grib2nc --input "/path/to/source/grib" --output "/output/20160226T120000" --reference_time "2016-02-26T12:00:00+0000" --template_directory "/template"'
        self.create_adapter()
        resource = mock.MagicMock()
        resource.data.productinstance.reference_time = datetime.datetime(2016, 2, 26, 12, 0, 0, tzinfo=dateutil.tz.tzutc())
        resource.data.productinstance.version = 1
        resource.data.time_period_begin = datetime.datetime(2016, 2, 26, 12, 0, 0, tzinfo=dateutil.tz.tzutc())
        resource.data.time_period_begin = datetime.datetime(2016, 2, 29, 3, 0, 0, tzinfo=dateutil.tz.tzutc())
        resource.expires = datetime.datetime(2016, 3, 1, 12, 0, 0, tzinfo=dateutil.tz.tzutc())
        resource.url = 'file:///path/to/source/grib'
        job = self.adapter.create_job(uuid.uuid4(), resource)
        command = re.sub(r'[\s$]+', ' ', job.command.strip(), re.MULTILINE)
        self.assertEqual(command, compare_command)
