"""!
@brief Configuration module.

This module contains classes necessary for automatic configuration of objects
from a configuration file.
"""

import eva.exceptions


# Environment variables in this list will be censored in the log output.
SECRET_CONFIGURATION = [
    'api_key',
]


class ConfigurableObject(object):
    """!
    @brief Base class that allows the subclass to define a list of required and
    optional configuration environment variables.

    In order to use classes in your EVA configuration, they MUST be derived
    from this class. Optionally, they may reimplement the `_factory()` method,
    which must return a configured class instance of any type.

    Usage:

        incubator, object_ = eva.config.ConfigurableObject().factory(
            <configparser.ConfigParser instance>,
            <section header>,
            [<section header>],
            [...]
        )

        # `incubator` is a reference to the ConfigurableObject instance, while
        # `object_` is a reference to the object returned by _factory().
        # Normally, you proceed with calling init() if your instantiated class
        # is a ConfigurableObject instance:

        object_.init()

    Configurable variables are defined with:

        CONFIG = {
            'foo': {
                'type': 'list_int',
                'help': 'Description of what this setting does',
                'default': '1,2,3',
            },
            'bar': {
                ...
            },
        },

    The types are defined as `normalize_config_<type>` functions in this class.
    Subclasses might implement their own normalization functions as needed.

    To define configuration options as required or optional, you may do:

        REQUIRED_CONFIG = ['foo']
        OPTIONAL_CONFIG = ['bar']

    Note that variables that are not in any of these two lists will be regarded
    as invalid options, and trying to use them will throw an exception.

    The normalized variables are made available in the `env` class member. For
    instance:

        assert object_.env['foo'] == [1, 2, 3]  # True

    """

    ## Hash with available configuration variables.
    CONFIG = {}

    ## List of required configuration variables.
    REQUIRED_CONFIG = []

    ## List of optional configuration variables.
    OPTIONAL_CONFIG = []

    @classmethod
    def factory(self, config, *args):
        """!
        @brief Load the specified configuration data, according to
        load_configuration, and return a tuple of the incubator class and the
        instantiated class.
        """
        object_ = self()
        object_.load_configuration(config, *args)
        object_.set_config_id(args[-1])
        return (object_, object_._factory())

    def _factory(self):
        """!
        @brief Return a class instance. Override this method in order to return
        a different object than the eva.ConfigurableObject instance.
        @returns object
        """
        return self

    def set_config_id(self, id):
        """!
        @brief Set the configuration ID of this class.
        """
        if hasattr(self, '_config_id'):
            raise RuntimeError('Configuration ID for this class already set!')
        self._config_id = id

    @property
    def config_id(self):
        """!
        @brief Return the configuration ID of this class.
        """
        return self._config_id

    def init(self):
        """!
        @brief Provides a place for subclasses to run their own initialization.
        """
        pass

    @classmethod
    def format_help(class_):
        """!
        @brief Format a help string with this class' configuration variables.
        """
        output = ['%s configuration:' % class_.__name__]
        for key in sorted(class_.CONFIG.keys()):
            output += ['  %s  (default: %s)' % (key, class_.CONFIG[key]['default'])]
            output += ['      %s' % class_.CONFIG[key]['help']]
        return '\n'.join(output)

    def normalize_config_string(self, value):
        """!
        Coerce a type into a unicode string.
        """
        return str(value)

    def normalize_config_int(self, value):
        """!
        Coerce a type into an integer.
        """
        if len(value) == 0:
            return None
        return int(value)

    def normalize_config_positive_int(self, value):
        """!
        Coerce a type into an integer.
        """
        value = self.normalize_config_int(value)
        if value <= 0:
            raise eva.exceptions.InvalidConfigurationException('Invalid non-positive integer: %d' % value)
        return value

    def normalize_config_null_bool(self, value):
        """!
        Coerce a type into a unicode string.
        """
        return eva.parse_boolean_string(value)

    def normalize_config_bool(self, value):
        """!
        Coerce a type into a unicode string.
        """
        v = self.normalize_config_null_bool(value)
        if v is None:
            raise eva.exceptions.InvalidConfigurationException('Invalid boolean value')
        return v

    def normalize_config_list(self, value):
        """!
        Split a comma-separated string into a list.
        """
        return eva.split_comma_separated(value)

    def normalize_config_list_string(self, value):
        """!
        Split a comma-separated string into a list of unicode strings.
        """
        return [self.normalize_config_string(x) for x in self.normalize_config_list(value) if len(x) > 0]

    def normalize_config_list_int(self, value):
        """!
        Split a comma-separated string into a list of integers.
        """
        return [self.normalize_config_int(x) for x in self.normalize_config_list(value)]

    def normalize_config_config_class(self, value):
        """!
        @brief Set a dotted class name into a resolvable dependency.
        """
        return ResolvableDependency(value)

    def resolve_dependencies(self, config_classes):
        """!
        @brief Replace all configuration objects with their true class reference.
        """
        for key in self.env.keys():
            if isinstance(self.env[key], ResolvableDependency):
                self.env[key] = self.env[key].resolve(config_classes)

    def load_configuration(self, config, *args):
        """!
        @brief Normalize input configuration based on the configuration
        definition: split strings into lists, convert to types.
        """
        ## Dictionary of normalized, configured variables.
        self.env = {}
        keys = list(set(self.REQUIRED_CONFIG + self.OPTIONAL_CONFIG))
        configured = []

        # Iterate through required and optional config, and read only those variables
        for key in keys:
            if key not in self.CONFIG:
                raise RuntimeError(
                    "Missing configuration option '%s' in adapter CONFIG hash, please fix your code!" % key
                )

            # Read default value and enforce requirements
            found = False
            for section in args:
                if section not in config:
                    continue
                if key not in config[section]:
                    continue
                value = config.get(section, key)
                found = True

            if not found:
                if key in self.REQUIRED_CONFIG:
                    raise eva.exceptions.MissingConfigurationException(
                        'Missing required environment variable %s (%s)' % (key, self.CONFIG[key]['help'])
                    )
                if key in self.OPTIONAL_CONFIG:
                    value = self.CONFIG[key]['default']

            # Normalize configuration option
            option_type = self.CONFIG[key]['type']
            func_name = 'normalize_config_' + option_type
            try:
                func = getattr(self, func_name)
            except:
                raise RuntimeError("No normalization function for configuration type '%s' found!" % option_type)
            try:
                value = func(value)
            except Exception as e:
                raise eva.exceptions.InvalidConfigurationException("Invalid value '%s' for configuration '%s': %s", value, key, e)

            # Write normalized value into configuration hash
            self.env[key] = value
            configured += [key]

        # Check for extraneous options, and raise an exception if a non-defined key is set
        for section in args:
            if section not in config:
                continue
            for key in config[section]:
                if key not in configured:
                    raise eva.exceptions.InvalidConfigurationException("Invalid configuration key '%s' for class '%s'" % (key, self.__class__.__name__))

    def format_config(self):
        """!
        @brief Return a list of strings with configuration options, formatted
        in "key=value" format, and censored variables filtered out.
        """
        strings = []
        for key, var in sorted(self.env.items()):
            if key in SECRET_CONFIGURATION:
                var = '****CENSORED****'
            strings += ["%s=%s" % (key, var)]
        return strings


class ResolvableDependency(object):
    """!
    @brief A string representation of a configuration key, which will be
    converted into an object as soon as all the configuration keys have been
    loaded.
    """
    def __init__(self, key):
        self.key = key

    def resolve(self, config_classes):
        try:
            return config_classes[self.key]
        except KeyError:
            raise eva.exceptions.InvalidConfigurationException(
                "Cannot resolve class dependencies: section '%s' is not found in the configuration." %
                self.key
            )

    def __str__(self):
        return self.key
