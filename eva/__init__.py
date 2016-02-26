import logging
import time

import eva.exceptions


class ConfigurableObject(object):
    """
    Base class that allows the subclass to define a list of required and
    optional configuration environment variables.

    The subclass has the responsibility of calling `validate_configuration()`.

    Variables are configured as such:

        CONFIG = {
            'EVA_VARIABLE_FOO': 'Description of what this setting does',
            'EVA_FOO_BAR': 'Helpful description of what the other setting does',
        }

    Then, to use them either as required or optional variables, you may do:

        REQUIRED_CONFIG = ['EVA_VARIABLE']
        OPTIONAL_CONFIG = ['EVA_FOO_BAR']

    """

    # @brief Hash with available configuration variables.
    CONFIG = {
    }

    # @brief List of required configuration variables.
    REQUIRED_CONFIG = []

    # @brief List of optional configuration variables.
    OPTIONAL_CONFIG = []

    def validate_configuration(self):
        """
        @brief Throw an exception if all required environment variables are not set.
        """
        errors = 0
        for key in self.REQUIRED_CONFIG:
            if key not in self.env or self.env[key] is None:
                logging.critical('Missing required environment variable %s (%s)', key, self.CONFIG[key])
                errors += 1
        for key in self.OPTIONAL_CONFIG:
            if key not in self.env or self.env[key] is None:
                logging.debug('Optional environment variable not configured: %s (%s)', key, self.CONFIG[key])
                self.env[key] = None
        if errors > 0:
            raise eva.exceptions.MissingConfigurationException('Missing %d required environment variables' % errors)


def retry_n(func, args=(), kwargs={}, interval=5, exceptions=(Exception,), warning=1, error=3, give_up=5):
    """
    Call 'func' and, if it throws anything listed in 'exceptions', catch it and retry again
    up to 'give_up' times. If give_up is <= 0, retry indefinitely.
    Checks that error > warning > 0, and give_up > error or give_up <= 0.
    """
    assert (warning > 0) and (error > warning) and (give_up <= 0 or give_up > error)
    tries = 0
    while True:
        try:
            return func(*args, **kwargs)
        except exceptions, e:
            tries += 1
            if give_up > 0 and tries >= give_up:
                logging.error('Action failed %d times, giving up: %s' % (give_up, e))
                return False
            if tries >= error:
                logfunc = logging.error
            elif tries >= warning:
                logfunc = logging.warning
            else:
                logfunc = logging.info
            logfunc('Action failed, retrying in %d seconds: %s' % (interval, e))
            time.sleep(interval)


def in_array_or_empty(id, array):
    """
    @returns true if `id` is found in `array`, or `array` is empty.
    """
    return (len(array) == 0) or (id in array)


def url_to_filename(url):
    """
    @brief Convert a file://... URL to a path name. Raises an exception if
    the URL does not start with file://.
    """
    start = 'file://'
    if not url.startswith(start):
        raise RuntimeError('Expected an URL starting with %s, got %s instead' % (start, url))
    return url[len(start):]
