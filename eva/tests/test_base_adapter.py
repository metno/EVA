import unittest
import logging
import datetime
import mock

import productstatus
import productstatus.api
import productstatus.event

import eva.mail
import eva.adapter
import eva.executor
import eva.statsd


BLANK_UUID = '00000000-0000-0000-0000-000000000000'
INVALID_UUID = 'foobarba-trol-olol-olol-abcdefabcdef'


class TestBaseAdapter(unittest.TestCase):

    def setUp(self):
        self.env = {}
        self.group_id = 'group-id'
        self.productstatus_api = productstatus.api.Api('http://localhost:8000')
        self.logger = logging
        self.zookeeper = None
        self.statsd = eva.statsd.StatsDClient()
        self.mailer = eva.mail.NullMailer()
        self.globe = eva.globe.Global(group_id=self.group_id,
                                      logger=self.logger,
                                      mailer=self.mailer,
                                      statsd=self.statsd,
                                      zookeeper=self.zookeeper,
                                      )
        self.executor = eva.executor.NullExecutor(None, self.env, self.globe)

    def create_adapter(self):
        self.adapter = eva.adapter.BaseAdapter(self.env, self.executor, self.productstatus_api, self.globe)

    def test_default_configuration_keys(self):
        self.create_adapter()
        self.assertListEqual(sorted(self.adapter.CONFIG.keys()), [
            'EVA_INPUT_DATA_FORMAT',
            'EVA_INPUT_PARTIAL',
            'EVA_INPUT_PRODUCT',
            'EVA_INPUT_REFERENCE_HOURS',
            'EVA_INPUT_SERVICE_BACKEND',
            'EVA_INPUT_WITH_HASH',
            'EVA_OUTPUT_BASE_URL',
            'EVA_OUTPUT_DATA_FORMAT',
            'EVA_OUTPUT_FILENAME_PATTERN',
            'EVA_OUTPUT_LIFETIME',
            'EVA_OUTPUT_PRODUCT',
            'EVA_OUTPUT_SERVICE_BACKEND',
            'EVA_PRODUCTSTATUS_API_KEY',
            'EVA_PRODUCTSTATUS_USERNAME',
            'EVA_REFERENCE_TIME_THRESHOLD',
            'EVA_SINGLE_INSTANCE',
        ])

    def test_additional_configuration_keys(self):
        class Foo(eva.adapter.BaseAdapter):
            CONFIG = {
                'EVA_TEST_FOO': 'Foo documentation',
            }
        self.adapter = Foo(self.env, self.executor, self.productstatus_api, self.globe)
        self.assertIn('EVA_TEST_FOO', self.adapter.CONFIG.keys())
        self.assertIn('EVA_INPUT_DATA_FORMAT', self.adapter.CONFIG.keys())

    def test_required_configuration_keys(self):
        class Foo(eva.adapter.BaseAdapter):
            REQUIRED_CONFIG = [
                'EVA_INPUT_PRODUCT',
            ]
        with self.assertRaises(eva.exceptions.MissingConfigurationException):
            self.adapter = Foo(self.env, self.executor, self.productstatus_api, self.globe)

    def test_optional_configuration_keys(self):
        class Foo(eva.adapter.BaseAdapter):
            OPTIONAL_CONFIG = [
                'EVA_INPUT_PRODUCT',
            ]
        self.adapter = Foo(self.env, self.executor, self.productstatus_api, self.globe)
        self.assertIn('EVA_INPUT_PRODUCT', self.adapter.env)
        self.assertEqual(self.adapter.env['EVA_INPUT_PRODUCT'], [])

    def test_read_config(self):
        class Foo(eva.adapter.BaseAdapter):
            REQUIRED_CONFIG = [
                'EVA_INPUT_PRODUCT',
                'EVA_INPUT_REFERENCE_HOURS',
                'EVA_INPUT_SERVICE_BACKEND',
            ]
            OPTIONAL_CONFIG = [
                'EVA_OUTPUT_SERVICE_BACKEND',
            ]
        self.env = {
            'EVA_INPUT_PRODUCT': BLANK_UUID,
            'EVA_INPUT_REFERENCE_HOURS': ' 00, 12,18',
            'EVA_INPUT_SERVICE_BACKEND': '%s,%s, %s' % (BLANK_UUID, BLANK_UUID, BLANK_UUID),
            'EVA_OUTPUT_SERVICE_BACKEND': BLANK_UUID,
        }
        self.adapter = Foo(self.env, self.executor, self.productstatus_api, self.globe)
        self.assertListEqual(self.adapter.env['EVA_INPUT_PRODUCT'], [BLANK_UUID])
        self.assertListEqual(self.adapter.env['EVA_INPUT_REFERENCE_HOURS'], [0, 12, 18])
        self.assertListEqual(self.adapter.env['EVA_INPUT_SERVICE_BACKEND'], [BLANK_UUID, BLANK_UUID, BLANK_UUID])
        self.assertEqual(self.adapter.env['EVA_OUTPUT_SERVICE_BACKEND'], BLANK_UUID)

    def test_productstatus_environment_variables(self):
        self.create_adapter()
        self.assertIn('EVA_PRODUCTSTATUS_USERNAME', self.adapter.OPTIONAL_CONFIG)
        self.assertIn('EVA_PRODUCTSTATUS_API_KEY', self.adapter.OPTIONAL_CONFIG)

    def test_has_productstatus_credentials(self):
        self.env = {
            'EVA_PRODUCTSTATUS_API_KEY': '5bcf851f09bc65043d987910e1448781fcf4ea12',
            'EVA_PRODUCTSTATUS_USERNAME': 'admin',
        }
        self.create_adapter()
        self.assertTrue(self.adapter.has_productstatus_credentials())

    def test_has_productstatus_credentials_fail(self):
        self.env = {
            'EVA_PRODUCTSTATUS_USERNAME': 'admin',
        }
        self.create_adapter()
        self.assertFalse(self.adapter.has_productstatus_credentials())

    def test_require_productstatus_credentials(self):
        self.env = {
            'EVA_PRODUCTSTATUS_API_KEY': '5bcf851f09bc65043d987910e1448781fcf4ea12',
            'EVA_PRODUCTSTATUS_USERNAME': 'admin',
        }
        self.create_adapter()
        self.adapter.require_productstatus_credentials()

    def test_require_productstatus_credentials_fail(self):
        self.env = {
            'EVA_PRODUCTSTATUS_USERNAME': 'admin',
        }
        self.create_adapter()
        with self.assertRaises(eva.exceptions.MissingConfigurationException):
            self.adapter.require_productstatus_credentials()

    def test_post_to_productstatus(self):
        self.env = {
            'EVA_PRODUCTSTATUS_API_KEY': '5bcf851f09bc65043d987910e1448781fcf4ea12',
            'EVA_PRODUCTSTATUS_USERNAME': 'admin',
        }
        eva.adapter.BaseAdapter.PRODUCTSTATUS_REQUIRED_CONFIG = ['EVA_INPUT_WITH_HASH']
        self.create_adapter()
        self.assertFalse(self.adapter.post_to_productstatus())
        self.env['EVA_INPUT_WITH_HASH'] = 'YES'
        self.create_adapter()
        self.assertTrue(self.adapter.post_to_productstatus())

    def test_blacklist_add(self):
        self.create_adapter()
        self.assertFalse(self.adapter.is_blacklisted('abc'))
        self.adapter.blacklist_add('abc')
        self.assertTrue(self.adapter.is_blacklisted('abc'))

    def test_no_forward_to_uuid(self):
        self.create_adapter()
        self.assertTrue(self.adapter.is_in_required_uuids('abc'))
        self.assertTrue(self.adapter.is_in_required_uuids('def'))

    def test_forward_to_uuid(self):
        self.create_adapter()
        self.adapter.forward_to_uuid('abc')
        self.assertTrue(self.adapter.is_in_required_uuids('abc'))
        self.assertFalse(self.adapter.is_in_required_uuids('def'))

    def test_remove_required_uuid(self):
        self.create_adapter()
        self.adapter.forward_to_uuid('abc')
        self.assertFalse(self.adapter.is_in_required_uuids('def'))
        self.adapter.remove_required_uuid('abc')
        self.assertTrue(self.adapter.is_in_required_uuids('def'))

    def test_clear_required_uuid(self):
        self.create_adapter()
        self.adapter.forward_to_uuid('abc')
        self.adapter.forward_to_uuid('def')
        self.assertFalse(self.adapter.is_in_required_uuids('ghi'))
        self.adapter.clear_required_uuids()
        self.assertTrue(self.adapter.is_in_required_uuids('ghi'))

    def test_input_partial_true(self):
        class Foo(eva.adapter.BaseAdapter):
            REQUIRED_CONFIG = ['EVA_INPUT_PARTIAL']
        self.env['EVA_INPUT_PARTIAL'] = 'NO'
        self.adapter = Foo(self.env, self.executor, self.productstatus_api, self.globe)
        self.assertEqual(self.adapter.process_partial, self.adapter.PROCESS_PARTIAL_NO)

    def test_input_partial_false(self):
        class Foo(eva.adapter.BaseAdapter):
            REQUIRED_CONFIG = ['EVA_INPUT_PARTIAL']
        self.env['EVA_INPUT_PARTIAL'] = 'YES'
        self.adapter = Foo(self.env, self.executor, self.productstatus_api, self.globe)
        self.assertEqual(self.adapter.process_partial, self.adapter.PROCESS_PARTIAL_ONLY)

    def test_input_partial_both(self):
        class Foo(eva.adapter.BaseAdapter):
            REQUIRED_CONFIG = ['EVA_INPUT_PARTIAL']
        self.env['EVA_INPUT_PARTIAL'] = 'BOTH'
        self.adapter = Foo(self.env, self.executor, self.productstatus_api, self.globe)
        self.assertEqual(self.adapter.process_partial, self.adapter.PROCESS_PARTIAL_BOTH)

    def test_lifetime_none(self):
        class Foo(eva.adapter.BaseAdapter):
            OPTIONAL_CONFIG = ['EVA_OUTPUT_LIFETIME']
        self.adapter = Foo(self.env, self.executor, self.productstatus_api, self.globe)
        self.assertFalse(self.adapter.has_output_lifetime())
        self.assertIsNone(self.adapter.expiry_from_lifetime())

    def test_lifetime_zero(self):
        """!
        Note that this test might fail if your system is reeeeally slow.
        """
        class Foo(eva.adapter.BaseAdapter):
            OPTIONAL_CONFIG = ['EVA_OUTPUT_LIFETIME']
        self.env['EVA_OUTPUT_LIFETIME'] = '0'
        self.adapter = Foo(self.env, self.executor, self.productstatus_api, self.globe)
        self.assertTrue(self.adapter.has_output_lifetime())
        timediff = eva.now_with_timezone() - self.adapter.expiry_from_lifetime()
        self.assertEqual(int(timediff.total_seconds()), 0)

    def test_lifetime_positive(self):
        class Foo(eva.adapter.BaseAdapter):
            OPTIONAL_CONFIG = ['EVA_OUTPUT_LIFETIME']
        self.env['EVA_OUTPUT_LIFETIME'] = '24'
        self.adapter = Foo(self.env, self.executor, self.productstatus_api, self.globe)
        self.assertTrue(self.adapter.has_output_lifetime())
        expiry = self.adapter.expiry_from_lifetime()
        future = eva.now_with_timezone() + datetime.timedelta(days=1)
        self.assertEqual(future.date(), expiry.date())

    def test_single_instance_fails(self):
        self.env['EVA_SINGLE_INSTANCE'] = 'YES'
        with self.assertRaises(eva.exceptions.InvalidConfigurationException):
            self.create_adapter()

    def test_single_instance(self):
        self.env['EVA_SINGLE_INSTANCE'] = 'YES'
        self.zookeeper = mock.MagicMock()
        self.zookeeper.EVA_BASE_PATH = '/foo'
        self.zookeeper.create = mock.MagicMock()
        self.globe = eva.globe.Global(group_id=self.group_id,
                                      logger=self.logger,
                                      mailer=self.mailer,
                                      statsd=self.statsd,
                                      zookeeper=self.zookeeper,
                                      )
        self.create_adapter()
        self.zookeeper.create.assert_called_once_with('/foo/single_instance_lock', None, ephemeral=True)

    def test_reference_time_threshold(self):
        self.env['EVA_REFERENCE_TIME_THRESHOLD'] = '420'
        self.create_adapter()
        now = eva.now_with_timezone() - datetime.timedelta(seconds=420)
        then = self.adapter.reference_time_threshold()
        diff = now - then
        self.assertEqual(int(diff.total_seconds()), 0)

    def test_reference_time_threshold_zero_default(self):
        self.create_adapter()
        now = eva.now_with_timezone() - datetime.timedelta(days=420)
        then = self.adapter.reference_time_threshold()
        self.assertGreater(now, then)

    def test_resource_matches_hash_config(self):
        """!
        @brief Test that the EVA_INPUT_WITH_HASH properly filters a
        DataInstance by its `hash` field.
        """
        resource = mock.MagicMock()

        # default is None
        self.create_adapter()
        self.assertEqual(self.adapter.env['EVA_INPUT_WITH_HASH'], None)
        resource.hash = None
        self.assertTrue(self.adapter.resource_matches_hash_config(resource))
        resource.hash = 'd3b07384d113edec49eaa6238ad5ff00'
        self.assertTrue(self.adapter.resource_matches_hash_config(resource))

        # filter out resources without hash
        self.env['EVA_INPUT_WITH_HASH'] = 'YES'
        self.create_adapter()
        resource.hash = None
        self.assertFalse(self.adapter.resource_matches_hash_config(resource))
        resource.hash = 'd3b07384d113edec49eaa6238ad5ff00'
        self.assertTrue(self.adapter.resource_matches_hash_config(resource))

        # filter out resources with hash
        self.env['EVA_INPUT_WITH_HASH'] = 'NO'
        self.create_adapter()
        resource.hash = None
        self.assertTrue(self.adapter.resource_matches_hash_config(resource))
        resource.hash = 'd3b07384d113edec49eaa6238ad5ff00'
        self.assertFalse(self.adapter.resource_matches_hash_config(resource))

    def test_processing_failures(self):
        self.create_adapter()
        self.assertEqual(self.adapter.processing_failures('foo'), 0)
        self.adapter.incr_processing_failures('foo')
        self.assertEqual(self.adapter.processing_failures('foo'), 1)
        self.adapter.incr_processing_failures('foo')
        self.assertEqual(self.adapter.processing_failures('foo'), 2)
        self.adapter.incr_processing_failures('bar')
        self.assertEqual(self.adapter.processing_failures('bar'), 1)
        self.assertEqual(self.adapter.processing_failures('foo'), 2)
        self.adapter.set_processing_failures('baz', 44)
        self.assertEqual(self.adapter.processing_failures('foo'), 2)
        self.assertEqual(self.adapter.processing_failures('baz'), 44)
