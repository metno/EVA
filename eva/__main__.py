import os
import uuid
import signal
import sys
import logging
import logging.config
import argparse
import kazoo.client
import kazoo.exceptions
import configparser

import eva.adapter
import eva.config
import eva.eventloop
import eva.executor
import eva.globe
import eva.health
import eva.logger
import eva.mail
import eva.statsd


# Some modules are producing way too much DEBUG log, define them here.
NOISY_LOGGERS = [
    'kafka.client',
    'kafka.conn',
    'kafka.consumer',
    'kafka.consumer.subscription_state',
    'kafka.coordinator',
    'kafka.producer',
    'kazoo',
    'kazoo.client',
    'kazoo.protocol.connection',
    'paramiko',
]

EXIT_SUCCESS = 0
EXIT_INVALID_CONFIG = 1
EXIT_ALREADY_RUNNING = 2
EXIT_BUG = 255


class Main(eva.config.ConfigurableObject):

    CONFIG = {
        'listeners': {
            'type': 'list_string',
            'help': 'Comma separated Python class names of listeners that should be run',
            'default': 'eva.listener.RPCListener,eva.listener.ProductstatusListener',
        },
        'mailer': {
            'type': 'config_class',
            'help': 'Configured Mailer instance',
            'default': '',
        },
        'productstatus': {
            'type': 'config_class',
            'help': 'Configured Productstatus instance',
            'default': '',
        },
        'statsd': {
            'type': 'list_string',
            'help': 'Comma-separated list of StatsD endpoints in the form <host>:<port>',
            'default': '',
        },
        'zookeeper': {
            'type': 'string',
            'help': 'ZooKeeper endpoints in the form <host>:<port>[,<host>:<port>,[...]]/<path>',
            'default': '',
        },
    }

    OPTIONAL_CONFIG = [
        'listeners',
        'statsd',
        'zookeeper',
    ]

    REQUIRED_CONFIG = [
        'mailer',
        'productstatus',
    ]

    def __init__(self):
        self.args = None
        self.config_class = {}
        self.incubator_class = {}
        self.logger = logging.getLogger('root')
        self.zookeeper = None

    @property
    def config_classes(self):
        instances = []
        for key, instance in self.config_class.items():
            instances += [instance]
        return instances

    @property
    def adapters(self):
        instances = []
        for instance in self.config_classes:
            if isinstance(instance, eva.base.adapter.BaseAdapter):
                instances += [instance]
        return instances

    @property
    def executors(self):
        instances = []
        for instance in self.config_classes:
            if isinstance(instance, eva.base.executor.BaseExecutor):
                instances += [instance]
        return instances

    @property
    def listeners(self):
        instances = []
        for instance in self.config_classes:
            if isinstance(instance, eva.base.listener.BaseListener):
                instances += [instance]
        return instances

    @property
    def productstatus(self):
        return self.env['productstatus']

    @property
    def mailer(self):
        return self.env['mailer']

    def parse_args(self):
        parser = argparse.ArgumentParser()  # FIXME: epilog
        parser_rpc_group = parser.add_mutually_exclusive_group()
        parser_rpc_group.add_argument(
            '--process_all_in_product_instance',
            action='store',
            type=str,
            required=False,
            metavar='UUID',
            help='Process all DataInstance resources belonging to a specific ProductInstance',
        )
        parser_rpc_group.add_argument(
            '--process_data_instance',
            action='store',
            type=str,
            required=False,
            metavar='UUID',
            help='Process a single DataInstance resource',
        )
        parser.add_argument(
            '--debug',
            action='store_true',
            default=False,
            help='Print DEBUG log statements by default',
        )
        parser.add_argument(
            '--group-id',
            action='store',
            help='Manually set the EVA group id (DANGEROUS!)',
        )
        parser.add_argument(
            '--health-check-port',
            action='store',
            type=int,
            help='Run a HTTP health check server on all interfaces at the specified port',
        )
        parser.add_argument(
            '--config',
            action='append',
            required=True,
            type=str,
            help='Load the specified configuration file, or an entire directory structure of configuration files. Can be specified multiple times.',
        )
        parser.add_argument(
            '--log-config',
            action='store',
            type=str,
            help='Use the specified configuration file for logging configuration.',
        )
        self.args = parser.parse_args()

    @staticmethod
    def signal_handler(sig, frame):
        raise eva.exceptions.ShutdownException('Caught signal %d, exiting.' % sig)

    def setup_signals(self):
        """!
        @brief Set up signals to catch interrupts and exit cleanly.
        """
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def setup_basic_logging(self):
        """!
        @brief Set up a minimal logging configuration.
        """
        logging.basicConfig(format='%(asctime)s: (%(levelname)s) %(message)s',
                            datefmt='%Y-%m-%dT%H:%M:%S%Z',
                            level=logging.INFO)
        self.logger = logging.getLogger('root')

    def setup_logging(self):
        """!
        @brief Override default logging configuration.
        """
        # Read configuration from parsed configuration
        if self.args.log_config:
            logging.config.fileConfig(self.args.log_config, disable_existing_loggers=False)
            self.logger = logging.getLogger('root')

        # Set DEBUG loglevel if --debug passed
        if self.args.debug:
            self.logger.setLevel(logging.DEBUG)

        # Disable DEBUG logging on some noisy loggers
        for noisy_logger in NOISY_LOGGERS:
            logging.getLogger(noisy_logger).setLevel(logging.INFO)

    def get_config_filenames(self, filenames):
        """!
        @brief Given a list of directories or filenames, return a list of
        recursively discovered configuration files where the file names end
        with ".ini".
        """
        expanded_filenames = []
        for filename in filenames:
            if os.path.isdir(filename):
                for (path, dirs, files) in os.walk(filename):
                    for filename in files:
                        if filename[-4:] != '.ini':
                            continue
                        expanded_filenames += [os.path.realpath(os.path.join(path, filename))]
            elif os.path.isfile(filename) or os.path.islink(filename):
                expanded_filenames += [os.path.realpath(filename)]
        return expanded_filenames

    def setup_configuration(self):
        """!
        @brief Scan for and read all configuration files into the `self.config` object.
        """
        self.logger.info('Scanning for configuration files...')
        filenames = self.get_config_filenames(self.args.config)
        for filename in filenames:
            self.logger.info('Found configuration file: %s', filename)
        self.config = configparser.ConfigParser()
        self.logger.info('Reading all configuration files...')
        self.config.read(filenames)
        self.logger.info('Successfully read and parsed all configuration files.')
        for section in sorted(self.config.sections()):
            self.logger.debug('Found configuration section: %s', section)

    def setup_eva_configuration(self):
        """!
        @brief Read configuration parameters from the 'eva' section.
        """
        config = eva.config.resolved_config_section(self.config, 'eva')
        self.load_configuration(config, 'eva')
        self.set_config_id('eva')

    def setup_client_group_id(self):
        """!
        @brief Set up Kafka client and group id.
        """
        self.client_id = str(uuid.uuid4())
        self.group_id = str(uuid.uuid4()) if not self.args.group_id else self.args.group_id
        self.logger.info('Using client ID: %s', self.client_id)
        self.logger.info('Using group ID: %s', self.group_id)

    def setup_health_check_server(self):
        """!
        @brief Set up a simple HTTP server to answer health checks.
        """
        if self.args.health_check_port:
            self.health_check_server = eva.health.HealthCheckServer('0.0.0.0', self.args.health_check_port)
            self.logger.info('Started health check server on 0.0.0.0:%d', self.args.health_check_port)
        else:
            self.health_check_server = None
            self.logger.warning('Not running health check server!')

    def setup_statsd_client(self):
        """!
        @brief Set up a StatsD client that will send data to multiple servers simultaneously.
        """
        self.statsd = eva.statsd.StatsDClient({'application': self.group_id}, self.env['statsd'])
        if len(self.env['statsd']) > 0:
            self.logger.info('StatsD client set up with application tag "%s", sending data to: %s', self.group_id, ', '.join(self.env['statsd']))
        else:
            self.logger.warning('StatsD not configured, will not send metrics.')

    def setup_zookeeper(self):
        """!
        @brief Instantiate the Zookeeper client, if enabled.
        """
        if not self.env['zookeeper']:
            self.logger.warning('ZooKeeper not configured.')
            return

        self.logger.info('Setting up Zookeeper connection to %s', self.env['zookeeper'])
        tokens = self.env['zookeeper'].strip().split(u'/')
        server_string = tokens[0]
        base_path = os.path.join('/', os.path.join(*tokens[1:]))
        self.zookeeper = kazoo.client.KazooClient(
            hosts=server_string,
            randomize_hosts=True,
        )
        self.logger.info('Using ZooKeeper, base path "%s"', base_path)
        self.zookeeper.start()
        self.zookeeper.EVA_BASE_PATH = base_path
        self.zookeeper.ensure_path(self.zookeeper.EVA_BASE_PATH)

    def setup_instance_lock(self):
        """!
        @brief Check that we have a Zookeeper endpoint if EVA requires that
        only a single instance is running at any given time.
        """
        if not self.zookeeper:
            self.logger.warning('DANGEROUS: single instance lock disabled because ZooKeeper is not set up.')
            self.logger.warning('DO NOT RUN THIS INSTANCE IN PRODUCTION!')
            return
        lock_path = os.path.join(self.zookeeper.EVA_BASE_PATH, 'lock')
        try:
            self.logger.info('Creating a Zookeeper ephemeral node with path %s', lock_path)
            self.zookeeper.create(lock_path, None, ephemeral=True)
        except kazoo.exceptions.NodeExistsError:
            raise eva.exceptions.AlreadyRunningException('Another instance of EVA is already running against this ZooKeeper endpoint; aborting!')

    def instantiate_config_classes(self):
        """!
        @brief Run through the configuration files, search for classes, and
        instantiate them.

        The class to load is defined by `class = path.to.ClassName` under each
        section of the configuration file. Classes must be instances of
        eva.config.ConfigurableObject.

        The resulting class is added to the `self.config_class` dictionary.
        """
        self.logger.info('Instantiating classes from configuration file...')

        sections = self.config.sections()
        for section in sections:
            config = eva.config.resolved_config_section(self.config, section)
            abstract = ('abstract' in config and eva.config.ConfigurableObject.normalize_config_bool(config['abstract']))
            if 'class' not in config:
                self.logger.info("Ignoring non-class configuration section '%s'.", section)
                continue
            elif abstract:
                self.logger.info("Ignoring abstract configuration section '%s'.", section)
                continue
            else:
                raise eva.exceptions.InvalidConfigurationException(
                    "Invalid configuration section '%s': not a class instance, and not defined as abstract" % section
                )
            class_name = config['class']
            del config['class']
            self.logger.info("Instantiating '%s' from configuration section '%s'.", class_name, section)
            class_type = eva.import_module_class(class_name)
            if not issubclass(class_type, eva.config.ConfigurableObject):
                raise eva.exceptions.InvalidConfigurationException(
                    "The class '%s' is not a subclass of eva.config.ConfigurableObject." % class_name
                )
            incubator_class = class_type()
            incubator, instance = incubator_class.factory(config, section)
            self.incubator_class[section] = incubator
            self.config_class[section] = instance

        # Hack to make this class a member of configuration classes, in order
        # to simplify further setup
        self.config_class['eva'] = self
        self.incubator_class['eva'] = self

        self.logger.info('Finished instantiating classes from configuration file.')

    def resolve_config_class_dependencies(self):
        """!
        @brief Resolve class dependency references across classes instantiated
        from the configuration files.
        """
        self.logger.info('Resolving class dependencies...')

        for key, instance in self.config_class.items():
            if not isinstance(instance, eva.config.ConfigurableObject):
                continue
            self.logger.info("Resolving dependencies for '%s'...", key)
            instance.resolve_dependencies(self.config_class)

        self.logger.info('Finished resolving class dependencies.')

    def init_config_classes(self):
        """!
        @brief Run initialization procedures on all classes instantiated from
        the configuration files.
        """
        self.logger.info('Initializing classes...')

        for instance in self.config_classes:
            if not isinstance(instance, eva.config.ConfigurableObject):
                continue
            self.logger.info("Initializing '%s'...", instance)
            if isinstance(instance, eva.globe.GlobalMixin):
                instance.set_globe(self.globe)
            instance.init()

        self.logger.info('Finished initializing classes.')

    def setup_listeners(self):
        """!
        @brief Configure all message listeners with a client ID.
        """
        self.logger.info('Setting up listeners...')

        for listener in self.listeners:
            self.logger.info("Listener: '%s'", listener)
            listener.set_kwargs(
                client_id=self.client_id,
                productstatus_api=self.env['productstatus'],
            )
            listener.setup_listener()

        self.logger.info('Finished setting up listeners.')

    def setup_globe(self):
        """!
        @brief Instantiate the Global class, populating it with instances of
        useful services such as logging and mailing.
        """
        self.globe = eva.globe.Global(group_id=self.group_id,
                                      logger=self.logger,
                                      mailer=self.mailer,
                                      productstatus=self.productstatus,
                                      statsd=self.statsd,
                                      zookeeper=self.zookeeper,
                                      )

    def print_configuration(self):
        """!
        @brief Print all configuration to log.
        """
        self.logger.info('*** COMPLETE SET OF CONFIGURATION VARIABLES ***')
        lines = []
        for key, instance in self.incubator_class.items():
            config = instance.format_config()
            lines += [key + '.' + line for line in config]
        for line in sorted(lines):
            self.logger.info(line)
        self.logger.info('*** END OF CONFIGURATION VARIABLES ***')

    def print_adapters(self):
        """!
        @brief Print configured adapters to log.
        """
        for adapter in self.adapters:
            self.logger.info('Configured adapter: %s', adapter)

    def setup(self):
        try:
            # Basic pre-flight setup: argument parsing, signals, logging setup
            self.setup_basic_logging()
            self.setup_signals()
            self.parse_args()
            self.setup_logging()

            self.logger.info('Starting EVA: the EVent Adapter.')

            # Read all configuration from files
            self.setup_configuration()
            self.setup_eva_configuration()

            # Start the health check server
            self.setup_health_check_server()

            # Set up objects required for the Global class
            self.setup_client_group_id()
            self.setup_statsd_client()
            self.setup_zookeeper()

            # Instantiate, link and initialize everything
            self.instantiate_config_classes()
            self.print_configuration()
            self.resolve_config_class_dependencies()
            self.setup_globe()
            self.init_config_classes()
            self.print_adapters()

            # Abort if EVA is already running
            self.setup_instance_lock()

            # Start listener classes
            self.setup_listeners()

            # Warn about configuration that might not be suitable
            if len(self.adapters) == 0:
                self.logger.warning('The current configuration does not include any adapter classes. Events will be processed, but no jobs will be generated!')
            if len(self.listeners) == 0:
                self.logger.warning('The current configuration does not include any listener classes. No events will reach EVA!')

        except eva.exceptions.ConfigurationException as e:
            self.logger.critical(str(e))
            self.logger.info('Shutting down EVA due to missing or invalid configuration.')
            self.exit(EXIT_INVALID_CONFIG)

        except eva.exceptions.AlreadyRunningException as e:
            self.logger.critical(str(e))
            self.exit(EXIT_ALREADY_RUNNING)

        except Exception as e:
            eva.print_and_mail_exception(e, self.logger, self.mailer)
            self.logger.critical('EVA initialization failed. Your code is broken, please fix it.')
            self.exit(EXIT_BUG)

    def start(self):
        self.statsd.incr('eva_start')

        try:
            evaloop = eva.eventloop.Eventloop(self.adapters,
                                              self.listeners,
                                              self.health_check_server,
                                              )
            evaloop.set_globe(self.globe)
            evaloop.init()
            evaloop.restore_queue()

            if self.args.process_all_in_product_instance or self.args.process_data_instance:
                evaloop.listeners = []
                if self.args.process_all_in_product_instance:
                    product_instance = self.productstatus.productinstance[self.args.process_all_in_product_instance]
                    evaloop.process_all_in_product_instance(product_instance)
                elif self.args.process_data_instance:
                    evaloop.process_data_instance(self.args.process_data_instance)
                while evaloop.main_loop_iteration():
                    continue
            else:
                evaloop()

        except eva.exceptions.ShutdownException as e:
            self.logger.info(str(e))
        except kazoo.exceptions.ConnectionLoss as e:
            self.logger.critical('Shutting down EVA due to ZooKeeper connection loss: %s', str(e))
            self.statsd.incr('zookeeper_connection_loss')
        except Exception as e:
            eva.print_and_mail_exception(e, self.logger, self.mailer)
            self.exit(EXIT_BUG)

        if self.zookeeper:
            self.logger.info('Stopping ZooKeeper.')
            self.zookeeper.stop()
        self.logger.info('Shutting down EVA.')
        self.exit(EXIT_SUCCESS)

    def exit(self, exit_code):
        if hasattr(self, 'statsd'):
            self.statsd.incr('eva_shutdown', tags={'exit_code': exit_code})
        sys.exit(exit_code)


if __name__ == "__main__":
    m = Main()
    m.setup()
    m.start()
