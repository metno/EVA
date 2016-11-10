"""!
@brief This module serves as a configuration endpoint for classes that are not
in the EVA namespace.
"""

import eva

import productstatus.api


class Incubator(eva.ConfigurableObject):
    """!
    @brief Base class with a factory method, which should instantiate an object
    and configure it according to the given configuration.
    """

    @classmethod
    def factory(self, config, *args):
        object_ = self()
        object_.load_configuration(config, *args)
        return object_._factory()

    def _factory(self):
        raise NotImplementedError('Please implement the factory() method in Incubator derived classes.')

    def init(self):
        """!
        @brief Provides a place for subclasses to run their own initialization.
        """
        pass


class ResolvableDependency(object):
    """!
    @brief A string representation of a configuration key, which will be
    converted into an object as soon as all the configuration keys have been
    loaded.
    """
    def __init__(self, key):
        self.key = key

    def resolve(self, config_classes):
        return config_classes[key]


class Productstatus(Incubator):
    CONFIG = {
        'url': {
            'type': 'string',
            'help': 'URL to Productstatus service',
            'default': 'https://productstatus.met.no',
        },
        'username': {
            'type': 'string',
            'help': 'Productstatus username for authentication',
            'default': '',
        },
        'api_key': {
            'type': 'string',
            'help': 'Productstatus API key matching the username',
            'default': '',
        },
        'verify_ssl': {
            'type': 'bool',
            'help': 'Set this option to skip Productstatus SSL certificate verification',
            'default': 'YES',
        },
    }

    OPTIONAL_CONFIG = [
        'api_key',
        'url',
        'username',
        'verify_ssl',
    ]

    def _factory(self):
        return productstatus.api.Api(
            self.env['url'],
            username=self.env['username'],
            api_key=self.env['api_key'],
            verify_ssl=self.env['verify_ssl'],
            timeout=10,
        )
