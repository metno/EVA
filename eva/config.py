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


def resolved_config_section(config, section, section_keys=None, ignore_defaults=False):
    """!
    @brief Recursively pull in includes and defaults for a configuration
    section, and combine them into a single dictionary. Provides infinite
    recursion protection.
    @param section configparser.ConfigParser A ConfigParser object instance.
    @param section_keys list List of section keys already parsed, used for infinite recursion protection.
    @param ignore_defaults bool Whether or not to include the 'defaults.<SECTION-BASENAME>' configuration section.
    @returns dict Dictionary of configuration values.
    """
    resolved = {}
    # Workaround greedy default parameter value
    # http://stackoverflow.com/questions/1132941/least-astonishment-and-the-mutable-default-argument
    if section_keys is None:
        section_keys = []
    section_keys += [section]
    section_keys.sort()

    if sorted(list(set(section_keys))) != section_keys:
        raise RuntimeError('Multiple inheritance of the same base object detected: %s' % section)

    if 'include' in config[section]:
        sections = eva.config.ConfigurableObject.normalize_config_list_string(config[section]['include'])
        for base_section in sections:
            resolved.update(resolved_config_section(config, base_section, section_keys=section_keys, ignore_defaults=True))

    if not ignore_defaults:
        section_defaults = 'defaults.' + section.split('.')[0]
        if section_defaults in config:
            resolved.update(resolved_config_section(config, section_defaults, section_keys=section_keys, ignore_defaults=True))

    if 'abstract' in resolved:
        del resolved['abstract']

    resolved.update(config[section])

    for key in ['class', 'include']:
        if key in resolved:
            del resolved[key]

    return resolved


class ConfigurableObject(object):
    """!
    @brief Base class that allows the subclass to define a list of required and
    optional configuration environment variables.

    In order to use classes in your EVA configuration, they MUST be derived
    from this class. Optionally, they may reimplement the `_factory()` method,
    which must return a configured class instance of any type.

    Usage:

        incubator, object_ = eva.config.ConfigurableObject().factory(
            <dictionary with configuration options>
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
    def factory(self, config, config_id):
        """!
        @brief Load the specified configuration data, according to
        load_configuration(), and return a tuple of the incubator class and the
        instantiated class.
        """
        object_ = self()
        object_.load_configuration(config)
        object_.set_config_id(config_id)
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

    def format_help(self):
        """!
        @brief Format a help string with this class' configuration variables.
        """
        output = ['%s configuration:' % self.__name__]
        for key in sorted(self.CONFIG.keys()):
            output += ['  %s  (default: %s)' % (key, self.CONFIG[key]['default'])]
            output += ['      %s' % self.CONFIG[key]['help']]
        return '\n'.join(output)

    @staticmethod
    def normalize_config_string(value):
        """!
        Coerce a type into a unicode string.
        """
        return str(value)

    @staticmethod
    def normalize_config_int(value):
        """!
        Coerce a type into an integer.
        """
        if len(value) == 0:
            return None
        return int(value)

    @staticmethod
    def normalize_config_positive_int(value):
        """!
        Coerce a type into an integer.
        """
        value = ConfigurableObject.normalize_config_int(value)
        if value <= 0:
            raise eva.exceptions.InvalidConfigurationException('Invalid non-positive integer: %d' % value)
        return value

    @staticmethod
    def normalize_config_null_bool(value):
        """!
        Coerce a type into a unicode string.
        """
        return eva.parse_boolean_string(value)

    @staticmethod
    def normalize_config_bool(value):
        """!
        Coerce a type into a unicode string.
        """
        v = ConfigurableObject.normalize_config_null_bool(value)
        if v is None:
            raise eva.exceptions.InvalidConfigurationException('Invalid boolean value')
        return v

    @staticmethod
    def normalize_config_list(value):
        """!
        Split a comma-separated string into a list.
        """
        return eva.split_comma_separated(value)

    @staticmethod
    def normalize_config_list_string(value):
        """!
        Split a comma-separated string into a list of unicode strings.
        """
        return [ConfigurableObject.normalize_config_string(x) for x in ConfigurableObject.normalize_config_list(value) if len(x) > 0]

    @staticmethod
    def normalize_config_list_int(value):
        """!
        Split a comma-separated string into a list of integers.
        """
        return [ConfigurableObject.normalize_config_int(x) for x in ConfigurableObject.normalize_config_list(value)]

    @staticmethod
    def normalize_config_config_class(value):
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
        @param config configparser.ConfigParser ConfigParser object.
        @param *args str ConfigParser sections to read from. Latter arguments take
        presedence and will overwrite values found in earlier sections.
        """
        ## Dictionary of normalized, configured variables.
        self.env = {}
        keys = list(set(self.REQUIRED_CONFIG + self.OPTIONAL_CONFIG))
        configured = []

        # Iterate through required and optional config, and read only those variables
        for key in keys:
            if key not in self.CONFIG:
                raise RuntimeError(
                    "Missing configuration option '%s' in class CONFIG hash, please fix your code!" % key
                )

            # Read default value and enforce requirements
            found = key in config
            if found:
                value = config[key]

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
        for key in config:
            if key not in configured:
                raise eva.exceptions.InvalidConfigurationException("Invalid configuration key '%s' for class '%s'" % (key, self.__class__.__name__))

    def format_config(self):
        """!
        @brief Return a list of strings with configuration options, formatted
        in `key=value` format, and censored variables filtered out.
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
