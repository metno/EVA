import configparser
import mock

import eva.config
import eva.event
import eva.eventqueue
import eva.exceptions
import eva.tests


class TestConfig(eva.tests.TestBase):
    def setUp(self):
        super().setUp()
        self.config = configparser.ConfigParser()

    def setup_with_config(self, object_class, config, *args):
        self.config.read_string(config)
        self.object_ = object_class()
        incubator, self.object_ = eva.config.ConfigurableObject().factory(self.config, *args)
        self.object_.init()

    def test_clean_init(self):
        config = \
"""
[object]
"""  # NOQA
        self.setup_with_config(eva.config.ConfigurableObject, config, 'object')

    def test_unsupported_option(self):
        config = \
"""
[object]
foo = bar
"""  # NOQA
        with self.assertRaises(eva.exceptions.InvalidConfigurationException):
            self.setup_with_config(eva.config.ConfigurableObject, config, 'object')
