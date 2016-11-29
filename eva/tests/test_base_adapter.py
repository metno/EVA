import datetime
from unittest import mock

import eva.adapter
import eva.executor
import eva.statsd
import eva.tests


BLANK_UUID = '00000000-0000-0000-0000-000000000000'
INVALID_UUID = 'foobarba-trol-olol-olol-abcdefabcdef'


class TestBaseAdapter(eva.tests.BaseTestAdapter):
    adapter_class = eva.adapter.BaseAdapter
    config_ini = \
"""
[adapter]
"""  # NOQA

    def test_default_configuration_keys(self):
        self.create_adapter()
        self.assertListEqual(sorted(self.adapter.CONFIG.keys()), [
            'concurrency',
            'executor',
            'input_data_format',
            'input_partial',
            'input_product',
            'input_reference_hours',
            'input_service_backend',
            'input_with_hash',
            'output_base_url',
            'output_data_format',
            'output_filename_pattern',
            'output_lifetime',
            'output_product',
            'output_service_backend',
            'reference_time_threshold',
        ])

    def test_additional_configuration_keys(self):
        class Foo(eva.adapter.BaseAdapter):
            CONFIG = {
                'test_foo': {
                    'type': 'int',
                    'help': 'bar',
                    'default': '1',
                },
            }
        self.create_adapter(adapter_class=Foo)
        self.assertIn('test_foo', self.adapter.CONFIG.keys())
        self.assertIn('input_data_format', self.adapter.CONFIG.keys())

    def test_required_configuration_keys(self):
        class Foo(eva.adapter.BaseAdapter):
            REQUIRED_CONFIG = [
                'input_product',
            ]
        with self.assertRaises(eva.exceptions.MissingConfigurationException):
            self.create_adapter(adapter_class=Foo)

    def test_optional_configuration_keys(self):
        class Foo(eva.adapter.BaseAdapter):
            OPTIONAL_CONFIG = [
                'input_product',
            ]
        self.create_adapter(adapter_class=Foo)
        self.assertIn('input_product', self.adapter.env)
        self.assertEqual(self.adapter.env['input_product'], [])

    def test_read_config(self):
        class Foo(eva.adapter.BaseAdapter):
            REQUIRED_CONFIG = [
                'input_product',
                'input_reference_hours',
                'input_service_backend',
            ]
            OPTIONAL_CONFIG = [
                'output_service_backend',
            ]

        self.config['adapter']['input_product'] = BLANK_UUID
        self.config['adapter']['input_reference_hours'] = ' 00, 12,18'
        self.config['adapter']['input_service_backend'] = '%s,%s, %s' % (BLANK_UUID, BLANK_UUID, BLANK_UUID)
        self.config['adapter']['output_service_backend'] = BLANK_UUID

        self.create_adapter(adapter_class=Foo)
        self.assertListEqual(self.adapter.env['input_product'], [BLANK_UUID])
        self.assertListEqual(self.adapter.env['input_reference_hours'], [0, 12, 18])
        self.assertListEqual(self.adapter.env['input_service_backend'], [BLANK_UUID, BLANK_UUID, BLANK_UUID])
        self.assertEqual(self.adapter.env['output_service_backend'], BLANK_UUID)

    def test_has_productstatus_credentials(self):
        self.setup_productstatus()
        self.create_adapter()
        self.assertTrue(self.adapter.has_productstatus_credentials())

    def test_has_productstatus_credentials_fail(self):
        self.productstatus.has_credentials = mock.Mock(return_value=False)
        self.create_adapter()
        self.assertFalse(self.adapter.has_productstatus_credentials())

    def test_require_productstatus_credentials(self):
        self.create_adapter()
        with self.assertRaises(RuntimeError):
            self.adapter.require_productstatus_credentials()

    def test_post_to_productstatus(self):
        eva.adapter.BaseAdapter.PRODUCTSTATUS_REQUIRED_CONFIG = ['input_with_hash']
        self.setup_productstatus()
        self.create_adapter()
        self.assertFalse(self.adapter.post_to_productstatus())
        self.config['adapter']['input_with_hash'] = 'YES'
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
            REQUIRED_CONFIG = ['input_partial']
        self.config['adapter']['input_partial'] = 'NO'
        self.create_adapter(adapter_class=Foo)
        self.assertEqual(self.adapter.process_partial, self.adapter.PROCESS_PARTIAL_NO)

    def test_input_partial_false(self):
        class Foo(eva.adapter.BaseAdapter):
            REQUIRED_CONFIG = ['input_partial']
        self.config['adapter']['input_partial'] = 'YES'
        self.create_adapter(adapter_class=Foo)
        self.assertEqual(self.adapter.process_partial, self.adapter.PROCESS_PARTIAL_ONLY)

    def test_input_partial_both(self):
        class Foo(eva.adapter.BaseAdapter):
            REQUIRED_CONFIG = ['input_partial']
        self.config['adapter']['input_partial'] = 'BOTH'
        self.create_adapter(adapter_class=Foo)
        self.assertEqual(self.adapter.process_partial, self.adapter.PROCESS_PARTIAL_BOTH)

    def test_lifetime_none(self):
        class Foo(eva.adapter.BaseAdapter):
            OPTIONAL_CONFIG = ['output_lifetime']
        self.create_adapter(adapter_class=Foo)
        self.assertFalse(self.adapter.has_output_lifetime())
        self.assertIsNone(self.adapter.expiry_from_lifetime())

    def test_lifetime_zero(self):
        """!
        Note that this test might fail if your system is reeeeally slow.
        """
        class Foo(eva.adapter.BaseAdapter):
            OPTIONAL_CONFIG = ['output_lifetime']
        self.config['adapter']['output_lifetime'] = '0'
        self.create_adapter(adapter_class=Foo)
        self.assertTrue(self.adapter.has_output_lifetime())
        timediff = eva.now_with_timezone() - self.adapter.expiry_from_lifetime()
        self.assertEqual(int(timediff.total_seconds()), 0)

    def test_lifetime_positive(self):
        class Foo(eva.adapter.BaseAdapter):
            OPTIONAL_CONFIG = ['output_lifetime']
        self.config['adapter']['output_lifetime'] = '24'
        self.create_adapter(adapter_class=Foo)
        self.assertTrue(self.adapter.has_output_lifetime())
        expiry = self.adapter.expiry_from_lifetime()
        future = eva.now_with_timezone() + datetime.timedelta(days=1)
        self.assertEqual(future.date(), expiry.date())

    def test_reference_time_threshold(self):
        self.config['adapter']['reference_time_threshold'] = '420'
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
        @brief Test that the input_with_hash properly filters a
        DataInstance by its `hash` field.
        """
        resource = mock.MagicMock()

        # default is None
        self.create_adapter()
        self.assertEqual(self.adapter.env['input_with_hash'], None)
        resource.hash = None
        self.assertTrue(self.adapter.resource_matches_hash_config(resource))
        resource.hash = 'd3b07384d113edec49eaa6238ad5ff00'
        self.assertTrue(self.adapter.resource_matches_hash_config(resource))

        # filter out resources without hash
        self.config['adapter']['input_with_hash'] = 'YES'
        self.create_adapter()
        resource.hash = None
        self.assertFalse(self.adapter.resource_matches_hash_config(resource))
        resource.hash = 'd3b07384d113edec49eaa6238ad5ff00'
        self.assertTrue(self.adapter.resource_matches_hash_config(resource))

        # filter out resources with hash
        self.config['adapter']['input_with_hash'] = 'NO'
        self.create_adapter()
        resource.hash = None
        self.assertTrue(self.adapter.resource_matches_hash_config(resource))
        resource.hash = 'd3b07384d113edec49eaa6238ad5ff00'
        self.assertFalse(self.adapter.resource_matches_hash_config(resource))
