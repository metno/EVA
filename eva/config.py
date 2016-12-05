"""
Configuration module, containing classes necessary for automatic configuration
of objects from a configuration file.
"""

import eva.exceptions


#: list of configuration options that shall be censored from log output.
SECRET_CONFIGURATION = [
    'api_key',
]


def resolved_config_section(config, section, section_keys=None, ignore_defaults=False):
    """
    Recursively pull in includes and defaults for a configuration section, and
    combine them into a single dictionary.

    This function provides infinite recursion protection, guaranteeing that a
    single configuration section will be read only once.

    :param configparser.ConfigParser section: reference to a configuration parser.
    :param list section_keys: list of section keys already parsed, used for infinite recursion protection.
    :param bool ignore_defaults: whether or not to include the 'defaults.<SECTION-BASENAME>' configuration section.
    :raises eva.exceptions.MissingConfigurationSectionException: when the configuration section does not exist.
    :rtype: dict
    :returns: dictionary of configuration values.
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

    if section not in config:
        raise eva.exceptions.MissingConfigurationSectionException("Configuration section '%s' was not found." % section)

    if 'include' in config[section]:
        sections = eva.config.ConfigurableObject.normalize_config_list_string(config[section]['include'])
        for base_section in sections:
            resolved.update(resolved_config_section(config, base_section, section_keys=section_keys, ignore_defaults=True))

    if not ignore_defaults:
        section_defaults = 'defaults.' + section.split('.')[0]
        if section_defaults in config:
            resolved.update(resolved_config_section(config, section_defaults, section_keys=section_keys, ignore_defaults=True))

    for key in ['abstract']:
        if key in resolved:
            del resolved[key]

    resolved.update(config[section])

    for key in ['include']:
        if key in resolved:
            del resolved[key]

    return resolved


class ConfigurableObject(object):
    """
    Base class that allows the subclass to define configuration options that
    can be populated from an INI file, along with a list of required and
    optional configuration variables.

    In order to use configuration classes in your EVA configuration, they MUST
    be derived from this class. Optionally, they may reimplement the
    :meth:`_factory()` method, which must return a configured class instance of
    any type.

    Usage:

    .. code-block:: python

       incubator, object_ = eva.config.ConfigurableObject().factory(
           {
               'foo': '1, 2, 3, 5, 7, 11',
               'bar': 'baz',
           }
       )

       # `incubator` is a reference to the ConfigurableObject instance, while
       # `object_` is a reference to the object returned by _factory().
       # Normally, you proceed with calling init() if your instantiated class
       # is a ConfigurableObject instance:

       object_.init()

    Configurable variables are defined with the :const:`CONFIG` dictionary.

    .. code-block:: python

       class MyClass(ConfigurableObject):
           CONFIG = {
               'foo': {
                   'type': 'list_int',
                   'default': '1,2,3',
               },
               'bar': {
                   ...
               },
           },

    The types are defined as ``normalize_config_<type>`` functions in this
    class. Subclasses might implement their own normalization functions as
    needed. A list of provided configuration normalizers are provided below:

        * :meth:`normalize_config_string`
        * :meth:`normalize_config_int`
        * :meth:`normalize_config_positive_int`
        * :meth:`normalize_config_null_bool`
        * :meth:`normalize_config_bool`
        * :meth:`normalize_config_list`
        * :meth:`normalize_config_list_string`
        * :meth:`normalize_config_list_int`
        * :meth:`normalize_config_config_class`

    To define configuration options as required or optional, you may do:

    .. code-block:: python

       class MyClass(ConfigurableObject):
           REQUIRED_CONFIG = ['foo']
           OPTIONAL_CONFIG = ['bar']

    Note that variables that are not in any of these two lists will be regarded
    as invalid options, and trying to use them will throw an exception.

    The normalized variables are made available in the `env` class member. For
    instance:

    .. code-block:: python

       assert object_.env['foo'] == [1, 2, 3]  # True
    """

    #: dictionary of possible configuration variables.
    CONFIG = {}

    #: list of required configuration variables.
    REQUIRED_CONFIG = []

    #: list of optional configuration variables.
    OPTIONAL_CONFIG = []

    def __repr__(self):
        """
        Return a string representation of the class.

        :rtype: str
        """
        return '<%s: %s>' % (self.__class__.__name__, self.config_id)

    @classmethod
    def factory(self, config, config_id):
        """
        Load the specified configuration data, according to :meth:`load_configuration()`.

        :param dict config: dictionary of configuration variables.
        :param str config_id: configuration section ID used to configure this class.
        :rtype: tuple
        :returns: a tuple of the incubator class instance, and the final instantiated class.
        """
        object_ = self()
        object_.load_configuration(config)
        object_.set_config_id(config_id)
        return (object_, object_._factory())

    def _factory(self):
        """
        Return a class instance. Override this method in order to return a
        different object than the :class:`ConfigurableObject` instance. This is
        useful if external objects are going to be instantiated.

        This method is called after :meth:`load_configuration`, so you can use
        the :attr:`env` dictionary for your object configuration.

        :rtype: object
        """
        return self

    def set_config_id(self, id):
        """
        Set the configuration ID of this class.

        :param str id: configuration ID to set.
        :raises RuntimeError: when the configuration ID is already set.
        """
        if hasattr(self, '_config_id'):
            raise RuntimeError('Configuration ID for this class already set!')
        self._config_id = id

    @property
    def config_id(self):
        """
        Return the configuration ID of this class.

        :rtype: str
        """
        if not hasattr(self, '_config_id'):
            return '(NO CONFIGURATION ID)'
        return self._config_id

    def isset(self, variable):
        """
        Returns True if ``variable`` is found in :attr:`env`, and its value is set.
        """
        return variable in self.env and bool(self.env[variable])

    def init(self):
        """
        Provides a place for subclasses to run their own initialization. By
        default, this method does nothing.
        """
        pass

    @staticmethod
    def normalize_config_string(value):
        """
        Convert a value of any type into a unicode string.

        :rtype: str
        """
        return str(value)

    @staticmethod
    def normalize_config_int(value):
        """
        Convert a value of any type into an integer.

        :rtype: int|None
        """
        if len(value) == 0:
            return None
        return int(value)

    @staticmethod
    def normalize_config_positive_int(value):
        """
        Convert a value of any type into a positive integer.

        :raises eva.exceptions.InvalidConfigurationException: when the integer is `<= 0`.
        :rtype: int
        """
        value = ConfigurableObject.normalize_config_int(value)
        if value <= 0:
            raise eva.exceptions.InvalidConfigurationException('Invalid non-positive integer: %d' % value)
        return value

    @staticmethod
    def normalize_config_null_bool(value):
        """
        Convert a value of any type into a nullable boolean.

        :rtype: bool|None
        """
        value = value.lower()
        if value in ['yes', 'true', 'on', '1']:
            return True
        if value in ['no', 'false', 'off', '0']:
            return False
        return None

    @staticmethod
    def normalize_config_bool(value):
        """
        Convert a value of any type into boolean.

        :raises eva.exceptions.InvalidConfigurationException: when a boolean value can not be derived.
        :rtype: bool
        """
        v = ConfigurableObject.normalize_config_null_bool(value)
        if v is None:
            raise eva.exceptions.InvalidConfigurationException('Invalid boolean value')
        return v

    @staticmethod
    def normalize_config_list(value):
        """
        Split a comma-separated string into a list.

        :rtype: list
        """
        return eva.split_comma_separated(value)

    @staticmethod
    def normalize_config_list_string(value):
        """
        Split a comma-separated string into a list of unicode strings. Empty
        values are ignored.

        :rtype: list
        """
        return [ConfigurableObject.normalize_config_string(x) for x in ConfigurableObject.normalize_config_list(value) if len(x) > 0]

    @staticmethod
    def normalize_config_list_int(value):
        """
        Split a comma-separated string into a list of integers.

        :rtype: list
        """
        return [ConfigurableObject.normalize_config_int(x) for x in ConfigurableObject.normalize_config_list(value)]

    @staticmethod
    def normalize_config_config_class(value):
        """
        Convert a dotted class name into a :class:`ResolvableDependency` object.

        :rtype: ResolvableDependency
        """
        return ResolvableDependency(value)

    def resolve_dependencies(self, config_classes):
        """
        Replace all configuration objects of type :class:`ResolvableDependency`
        with their true class reference.

        :param dict config_classes: dictionary with configuration IDs (see
               :attr:`config_id`) as keys pointing to
               :class:`ResolvableDependency` objects.
        :rtype: class
        """
        for key in self.env.keys():
            if isinstance(self.env[key], ResolvableDependency):
                self.env[key] = self.env[key].resolve(config_classes)

    def load_configuration(self, config):
        """
        Normalize input configuration based on the configuration definition:
        split strings into lists, convert to types.

        :param dict config: dictionary with configuration variables.
        :raises eva.exceptions.InvalidConfigurationException: when a configuration value is invalid (normalizer function raised an exception).
        :raises eva.exceptions.MissingConfigurationException: when a configuration value defined in :attr:`REQUIRED_CONFIG` is missing from the configuration dictionary.
        :raises RuntimeError: when a configuration variable is missing from the :attr:`CONFIG` hash, or has defined a normalization function that is not defined as a class member. This error indicates a bug in a subclass.
        """
        #: dictionary of normalized configuration variables, as read from the configuration file.
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
                        "Missing required configuration variable '%s'" % key
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
        """
        Generate a list of strings with configuration options, formatted in
        ``key=value`` format, and with censored variables defined in
        :attr:`SECRET_CONFIGURATION` filtered out.

        :rtype: list
        """
        strings = []
        for key, var in sorted(self.env.items()):
            if key in SECRET_CONFIGURATION:
                var = '****CENSORED****'
            strings += ["%s=%s" % (key, var)]
        return strings


class ResolvableDependency(object):
    """
    This class provides a future resolvable representation of a class defined
    in a configuration key, which will be converted into an object as soon as
    all the configuration keys have been loaded.

    :param str key: configuration ID (see :attr:`ConfigurableObject.config_id`) of the resolvable object.
    """

    def __init__(self, key):
        """
        """
        self.key = key

    def resolve(self, config_classes):
        """
        Instantiate the class, using the pre-defined :attr:`key` to look up the
        object in the :param:`config_classes` dictionary.

        :param dict config_classes: dictionary of classes.
        :rtype: class
        """
        try:
            return config_classes[self.key]
        except KeyError:
            raise eva.exceptions.InvalidConfigurationException(
                "Cannot resolve class dependencies: section '%s' is not found in the configuration." %
                self.key
            )

    def __str__(self):
        return self.key
