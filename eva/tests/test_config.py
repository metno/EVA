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
    ]


class TestConfig(eva.tests.TestBase):
    def setUp(self):
        super().setUp()
        self.config = configparser.ConfigParser()

    def setup_with_config(self, object_class, config, *args):
        self.config.read_string(config)
        incubator, self.object_ = object_class().factory(self.config, *args)
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
        self.setup_with_config(MockConfigObject, config, 'object')
        self.assertEqual(self.object_.env['string'], 'bar')
        self.assertEqual(self.object_.env['int'], 2)
        self.assertEqual(self.object_.env['positive_int'], 9)
        self.assertEqual(self.object_.env['null_bool'], None)
        self.assertEqual(self.object_.env['bool'], True)
        self.assertListEqual(self.object_.env['list'], ['a', 'b', '2'])
        self.assertListEqual(self.object_.env['list_string'], ['c', 'd', '3'])
        self.assertListEqual(self.object_.env['list_int'], [1, 2, 3, 5])

    def test_inheritance(self):
        config = \
"""
[inherit]
string = test

[object]
int = 3
"""  # NOQA
        self.setup_with_config(MockConfigObject, config, 'inherit', 'object')
        self.assertEqual(self.object_.env['string'], 'test')
        self.assertEqual(self.object_.env['int'], 3)

    def test_unsupported_option(self):
        config = \
"""
[object]
string = bar
"""  # NOQA
        with self.assertRaises(eva.exceptions.InvalidConfigurationException):
            self.setup_with_config(eva.config.ConfigurableObject, config, 'object')
