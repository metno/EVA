import logging

import eva.exceptions


class ConfigurableObject(object):
    """
    Base class that allows the subclass to define a list of required and
    optional configuration environment variables.

    The subclass has the responsibility of calling `validate_configuration()`.

    Variables are configured as such:

        REQUIRED_CONFIG = {
            'EVA_VARIABLE': 'Description of what this setting does',
        }
    """

    REQUIRED_CONFIG = {}
    OPTIONAL_CONFIG = {}

    def validate_configuration(self):
        """
        @brief Throw an exception if all required environment variables are not set.
        """
        errors = 0
        for key, helptext in self.REQUIRED_CONFIG.iteritems():
            if key not in self.env:
                logging.critical('Missing required environment variable %s (%s)', key, helptext)
                errors += 1
        for key, helptext in self.OPTIONAL_CONFIG.iteritems():
            if key not in self.env:
                logging.info('Optional environment variable not configured: %s (%s)', key, helptext)
                self.env[key] = None
        if errors > 0:
            raise eva.exceptions.MissingConfigurationException('Missing %d required environment variables' % errors)
