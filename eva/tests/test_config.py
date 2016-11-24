import configparser

import eva.config
import eva.event
import eva.eventqueue
import eva.exceptions
import eva.tests


class MockConfigObject(eva.config.ConfigurableObject):
    CONFIG = {
        'string': {
            'type': 'string',
            'help': '',
            'default': '',
        },
        'int': {
            'type': 'int',
            'help': '',
            'default': '-9',
        },
        'positive_int': {
            'type': 'positive_int',
            'help': '',
            'default': '9',
        },
        'null_bool': {
            'type': 'null_bool',
            'help': '',
            'default': '',
        },
        'bool': {
            'type': 'bool',
            'help': '',
            'default': 'YES',
        },
        'list': {
            'type': 'list',
            'help': '',
            'default': 'a,b,2',
        },
        'list_string': {
            'type': 'list_string',
            'help': '',
            'default': 'c,d,3',
        },
        'list_int': {
            'type': 'list_int',
            'help': '',
            'default': '1,2,3,5',
        },
        'config_class': {
            'type': 'config_class',
            'help': '',
            'default': 'class.foo',
        },
    }
    REQUIRED_CONFIG = ['string']
    OPTIONAL_CONFIG = [
        'int',
        'positive_int',
        'null_bool',
        'bool',
        'list',
        'list_string',
        'list_int',
        'config_class',
    ]


class TestConfig(eva.tests.TestBase):
    def setUp(self):
        super().setUp()
        self.config = configparser.ConfigParser()
        self.config_dict = {}

    def setup_with_config(self, object_class, config, section):
        self.config.read_string(config)
        self.config_dict = eva.config.resolved_config_section(self.config, section)
        incubator, self.object_ = object_class().factory(self.config_dict, section)
        self.object_.init()

    def test_clean_init(self):
        config = \
"""
[object]
"""  # NOQA
        self.setup_with_config(eva.config.ConfigurableObject, config, 'object')

    def test_supported_options(self):
        config = \
"""
[object]
string = bar
int = 2
"""  # NOQA
        config_class = {
            'class.foo': object(),
        }
        self.setup_with_config(MockConfigObject, config, 'object')
        self.assertEqual(self.object_.env['string'], 'bar')
        self.assertEqual(self.object_.env['int'], 2)
        self.assertEqual(self.object_.env['positive_int'], 9)
        self.assertEqual(self.object_.env['null_bool'], None)
        self.assertEqual(self.object_.env['bool'], True)
        self.assertListEqual(self.object_.env['list'], ['a', 'b', '2'])
        self.assertListEqual(self.object_.env['list_string'], ['c', 'd', '3'])
        self.assertListEqual(self.object_.env['list_int'], [1, 2, 3, 5])
        self.assertEqual(self.object_.env['config_class'].resolve(config_class), config_class['class.foo'])
        self.assertEqual(self.object_.config_id, 'object')

    def test_defaults(self):
        config = \
"""
[defaults.object]
string = test

[object]
int = 3
"""  # NOQA
        self.setup_with_config(MockConfigObject, config, 'object')
        self.assertEqual(self.object_.env['string'], 'test')
        self.assertEqual(self.object_.env['int'], 3)

    def test_resolved_config_section(self):
        config = \
"""
[defaults.object]
abstract = true
bool = no
include = include.foo
int = 1

[include.foo]
positive_int = 10
include = include.more

[include.bar]
null_bool = yes

[include.baz]
string = test
null_bool = no

[include.more]
list_int = 3, 4

[object]
int = 3
include = include.bar, include.baz
"""  # NOQA
        self.config.read_string(config)
        resolved = eva.config.resolved_config_section(self.config, 'object')
        test = {
            'bool': 'no',
            'int': '3',
            'list_int': '3, 4',
            'null_bool': 'no',
            'positive_int': '10',
            'string': 'test',
        }
        self.assertDictEqual(resolved, test)

    def test_unsupported_option(self):
        config = \
"""
[object]
string = bar
"""  # NOQA
        with self.assertRaises(eva.exceptions.InvalidConfigurationException):
            self.setup_with_config(eva.config.ConfigurableObject, config, 'object')

    def test_parse_boolean_string_true(self):
        self.assertTrue(eva.config.ConfigurableObject.normalize_config_bool('yes'))
        self.assertTrue(eva.config.ConfigurableObject.normalize_config_bool('YES'))
        self.assertTrue(eva.config.ConfigurableObject.normalize_config_bool('true'))
        self.assertTrue(eva.config.ConfigurableObject.normalize_config_bool('TRUE'))
        self.assertTrue(eva.config.ConfigurableObject.normalize_config_bool('True'))
        self.assertTrue(eva.config.ConfigurableObject.normalize_config_bool('ON'))
        self.assertTrue(eva.config.ConfigurableObject.normalize_config_bool('on'))
        self.assertTrue(eva.config.ConfigurableObject.normalize_config_bool('1'))

    def test_parse_boolean_string_false(self):
        self.assertFalse(eva.config.ConfigurableObject.normalize_config_bool('no'))
        self.assertFalse(eva.config.ConfigurableObject.normalize_config_bool('NO'))
        self.assertFalse(eva.config.ConfigurableObject.normalize_config_bool('false'))
        self.assertFalse(eva.config.ConfigurableObject.normalize_config_bool('FALSE'))
        self.assertFalse(eva.config.ConfigurableObject.normalize_config_bool('False'))
        self.assertFalse(eva.config.ConfigurableObject.normalize_config_bool('OFF'))
        self.assertFalse(eva.config.ConfigurableObject.normalize_config_bool('off'))
        self.assertFalse(eva.config.ConfigurableObject.normalize_config_bool('0'))
