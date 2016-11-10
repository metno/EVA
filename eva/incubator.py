"""!
@brief This module serves as a configuration endpoint for classes that are not
in the EVA namespace.
"""

import eva.config

import productstatus.api


class Productstatus(eva.config.ConfigurableObject):
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
