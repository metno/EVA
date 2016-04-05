import copy
import unittest
import datetime
import logging
import mock

import productstatus
import productstatus.api

import eva.adapter
import eva.executor
import eva.exceptions


INPUT_DATA_FORMAT_UUID = 'd5a7aad0-03f7-492e-b08c-bd142f31982d'
INPUT_PRODUCT_UUID = '1cfdbc88-6033-4e70-b404-8ae48167bb21'
INPUT_SERVICE_BACKEND_UUID = '8e92ce42-f363-44b4-b20e-280dedb9244a'


class TestDownloadAdapter(unittest.TestCase):

    def setUp(self):
        self.env = {
            'EVA_INPUT_DATA_FORMAT_UUID': INPUT_DATA_FORMAT_UUID,
            'EVA_INPUT_PRODUCT_UUID': INPUT_PRODUCT_UUID,
            'EVA_INPUT_SERVICE_BACKEND_UUID': INPUT_SERVICE_BACKEND_UUID,
            'EVA_OUTPUT_LIFETIME': 6,
            'EVA_PRODUCTSTATUS_API_KEY': '5bcf851f09bc65043d987910e1448781fcf4ea12',
            'EVA_PRODUCTSTATUS_USERNAME': 'admin',
        }
        self.productstatus_api = productstatus.api.Api('http://localhost:8000')
        self.logger = logging
        self.executor = eva.executor.NullExecutor(self.env, self.logger)

    def create_adapter(self):
        self.adapter = eva.adapter.CompleteAdapter(self.env, self.executor, self.productstatus_api, self.logger)

    def test_validate_prognosis_length(self):
        self.create_adapter()
        datainstance = mock.MagicMock()
        datainstance.data.productinstance.product.prognosis_length = 3
        self.adapter.validate_prognosis_length(datainstance)

    def test_validate_prognosis_length_null(self):
        self.create_adapter()
        datainstance = mock.MagicMock()
        datainstance.data.productinstance.product.prognosis_length = None
        with self.assertRaises(eva.exceptions.InvalidConfigurationException):
            self.adapter.validate_prognosis_length(datainstance)

    def test_validate_prognosis_length_zero(self):
        self.create_adapter()
        datainstance = mock.MagicMock()
        datainstance.data.productinstance.product.prognosis_length = 0
        with self.assertRaises(eva.exceptions.InvalidConfigurationException):
            self.adapter.validate_prognosis_length(datainstance)

    def test_validate_file_count(self):
        self.create_adapter()
        datainstance = mock.MagicMock()
        datainstance.data.productinstance.product.file_count = 1
        self.adapter.validate_file_count(datainstance)

    def test_validate_file_count_null(self):
        self.create_adapter()
        datainstance = mock.MagicMock()
        datainstance.data.productinstance.product.file_count = None
        with self.assertRaises(eva.exceptions.InvalidConfigurationException):
            self.adapter.validate_file_count(datainstance)

    def test_validate_file_count_zero(self):
        self.create_adapter()
        datainstance = mock.MagicMock()
        datainstance.data.productinstance.product.file_count = 0
        with self.assertRaises(eva.exceptions.InvalidConfigurationException):
            self.adapter.validate_file_count(datainstance)

    def test_check_time_period_equality_equal(self):
        self.create_adapter()
        datainstance = mock.MagicMock()
        begin = datetime.datetime.now()
        end = copy.copy(begin)
        datainstance.data.time_period_begin = begin
        datainstance.data.time_period_end = end
        self.assertEqual(self.adapter.check_time_period_equality(datainstance), True)

    def test_check_time_period_equality_differs(self):
        self.create_adapter()
        datainstance = mock.MagicMock()
        begin = datetime.datetime.now()
        end = datetime.datetime.now() - datetime.timedelta(minutes=1)
        datainstance.data.time_period_begin = begin
        datainstance.data.time_period_end = end
        self.adapter.check_time_period_equality(datainstance)
        self.assertEqual(self.adapter.check_time_period_equality(datainstance), False)

    def test_complete_time_period_begin(self):
        self.create_adapter()
        datainstance = mock.MagicMock()
        dt = datetime.datetime(year=2016,
                               month=1,
                               day=1,
                               hour=12,
                               minute=0,
                               second=0)
        datainstance.data.productinstance.reference_time = dt
        self.assertEqual(self.adapter.complete_time_period_begin(datainstance), dt)

    def test_complete_time_period_end(self):
        self.create_adapter()
        datainstance = mock.MagicMock()
        dt = datetime.datetime(year=2016,
                               month=1,
                               day=1,
                               hour=12,
                               minute=0,
                               second=0)
        compare = datetime.datetime(year=2016,
                                    month=1,
                                    day=1,
                                    hour=18,
                                    minute=0,
                                    second=0)
        datainstance.data.productinstance.reference_time = dt
        datainstance.data.productinstance.product.prognosis_length = 6
        self.assertEqual(self.adapter.complete_time_period_end(datainstance), compare)

    def test_complete_datainstance_exists(self):
        datainstances = mock.MagicMock()
        datainstances.count = mock.Mock(return_value=6)
        api = mock.MagicMock()
        api.datainstance.objects.filter = mock.Mock(return_value=datainstances)
        self.adapter = eva.adapter.CompleteAdapter(self.env,
                                                   self.executor,
                                                   api,
                                                   self.logger)
        begin = datetime.datetime(year=2016,
                                  month=1,
                                  day=1,
                                  hour=12,
                                  minute=0,
                                  second=0)
        end = datetime.datetime(year=2016,
                                month=1,
                                day=1,
                                hour=18,
                                minute=0,
                                second=0)

        format = mock.MagicMock()
        productinstance = mock.MagicMock()
        servicebackend = mock.MagicMock()

        datainstance = mock.MagicMock()
        datainstance.data.productinstance = productinstance
        datainstance.data.productinstance.reference_time = begin
        datainstance.data.productinstance.product.prognosis_length = 6
        datainstance.format = format
        datainstance.servicebackend = servicebackend

        params = {
            'data__time_period_begin': begin,
            'data__time_period_end': end,
            'format': format,
            'servicebackend': servicebackend,
            'data__productinstance': productinstance,
            'partial': False,
        }

        self.assertTrue(self.adapter.complete_datainstance_exists(datainstance))
        api.datainstance.objects.filter.assert_called_once_with(**params)
        datainstances.count = mock.Mock(return_value=0)
        self.assertFalse(self.adapter.complete_datainstance_exists(datainstance))

    def test_get_sibling_datainstances(self):
        api = mock.MagicMock()
        api.datainstance.objects.filter = mock.Mock(return_value=[])
        self.adapter = eva.adapter.CompleteAdapter(self.env,
                                                   self.executor,
                                                   api,
                                                   self.logger)
        format = mock.MagicMock()
        productinstance = mock.MagicMock()
        servicebackend = mock.MagicMock()

        datainstance = mock.MagicMock()
        datainstance.data.productinstance = productinstance
        datainstance.format = format
        datainstance.servicebackend = servicebackend
        datainstance.url = 'http://foo'

        params = {
            'format': format,
            'servicebackend': servicebackend,
            'data__productinstance': productinstance,
            'url': datainstance.url,
            'partial': True,
        }

        self.assertEqual(self.adapter.get_sibling_datainstances(datainstance), [])
        api.datainstance.objects.filter.assert_called_once_with(**params)

    def test_add_cache(self):
        self.create_adapter()
        resource = mock.MagicMock()
        resource.resource_uri = '/foo/'
        self.assertEqual(self.adapter.resource_cache, {})
        self.adapter.add_cache(resource)
        self.assertEqual(self.adapter.resource_cache[resource.resource_uri]['resource'], resource)
        self.assertGreater(self.adapter.resource_cache[resource.resource_uri]['expires'], datetime.datetime.now())

    def test_get_cache(self):
        self.create_adapter()
        resource = mock.MagicMock()
        resource.resource_uri = '/foo/'
        self.adapter.add_cache(resource)
        self.assertEqual(self.adapter.get_cache('/foo/'), resource)

    def test_clean_cache(self):
        self.create_adapter()
        resource = mock.MagicMock()
        resource.resource_uri = '/foo/'
        old = mock.MagicMock()
        old.resource_uri = '/old/'
        self.adapter.add_cache(resource)
        self.adapter.add_cache(old)
        self.adapter.resource_cache[old.resource_uri]['expires'] = datetime.datetime.now() - datetime.timedelta(minutes=1)
        self.assertEqual(len(self.adapter.resource_cache), 2)
        self.adapter.clean_cache()
        self.assertEqual(len(self.adapter.resource_cache), 1)
        with self.assertRaises(KeyError):
            self.adapter.get_cache(old.resource_uri)

    def test_cache_queryset(self):
        self.create_adapter()
        one = mock.MagicMock()
        one.resource_uri = '/one/'
        two = mock.MagicMock()
        two.resource_uri = '/two/'
        resources = [one, two]
        self.adapter.cache_queryset(resources)
        self.assertEqual(len(self.adapter.resource_cache), 2)
        self.assertEqual(self.adapter.get_cache(one.resource_uri), one)
        self.assertEqual(self.adapter.get_cache(two.resource_uri), two)

    def test_get_filtered_datainstances(self):
        self.create_adapter()
        one = mock.MagicMock()
        one.resource_uri = '/one/'
        one.data.time_period_begin = datetime.datetime(year=2016, month=1, day=1)
        one.data.time_period_end = datetime.datetime(year=2016, month=1, day=1)
        two = mock.MagicMock()
        two.resource_uri = '/two/'
        two.data.time_period_begin = datetime.datetime(year=2012, month=1, day=1)
        two.data.time_period_end = datetime.datetime(year=2012, month=1, day=8)
        three = mock.MagicMock()
        three.resource_uri = '/three/'
        three.data.time_period_begin = datetime.datetime(year=2008, month=1, day=9)
        three.data.time_period_end = datetime.datetime(year=2008, month=1, day=9)
        four = mock.MagicMock()
        four.resource_uri = '/four/'
        four.data.time_period_begin = datetime.datetime(year=2002, month=3, day=9)
        four.data.time_period_end = datetime.datetime(year=2002, month=3, day=9)
        resources = [one, two, three]
        self.adapter.cache_queryset(resources)
        self.adapter.cache_queryset([four])
        instances = self.adapter.get_filtered_datainstances(resources)
        self.assertEqual(len(instances), 2)
        self.assertEqual(instances[0], three)
        self.assertEqual(instances[1], one)

    def test_datainstance_set_has_correct_prognosis_length(self):
        self.create_adapter()
        one = mock.MagicMock()
        two = mock.MagicMock()
        three = mock.MagicMock()
        datainstance = mock.MagicMock()
        one.data.time_period_begin = datetime.datetime(year=2016,
                                                       month=1,
                                                       day=1,
                                                       hour=12,
                                                       minute=0,
                                                       second=0)
        three.data.time_period_end = datetime.datetime(year=2016,
                                                       month=1,
                                                       day=1,
                                                       hour=19,
                                                       minute=0,
                                                       second=0)
        datainstances = [one, two, three]
        datainstance.data.productinstance.product.prognosis_length = 7
        self.assertTrue(self.adapter.datainstance_set_has_correct_prognosis_length(datainstance, datainstances))
        datainstance.data.productinstance.product.prognosis_length = 5
        self.assertFalse(self.adapter.datainstance_set_has_correct_prognosis_length(datainstance, datainstances))

    def test_datainstance_set_has_correct_file_count(self):
        self.create_adapter()
        datainstance = mock.MagicMock()
        datainstance.data.productinstance.product.file_count = 4
        datainstances = [1, 2, 3, 4]
        self.assertTrue(self.adapter.datainstance_set_has_correct_file_count(datainstance, datainstances))
        datainstances = [1, 2, 3, 4, 5]
        self.assertFalse(self.adapter.datainstance_set_has_correct_file_count(datainstance, datainstances))
        datainstances = [1, 2]
        with self.assertRaises(eva.exceptions.InvalidConfigurationException):
            self.adapter.datainstance_set_has_correct_file_count(datainstance, datainstances)
