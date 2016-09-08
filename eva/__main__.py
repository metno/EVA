import os
import copy
import uuid
import signal
import sys
import logging
import logging.config
import argparse
import kazoo.client
import kazoo.exceptions

import productstatus
import productstatus.api
import productstatus.event

import eva
import eva.health
import eva.statsd
import eva.logger
import eva.eventloop
import eva.adapter
import eva.executor


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

EXIT_OK = 0
EXIT_INVALID_CONFIG = 1
EXIT_BUG = 255


class Main(eva.ConfigurableObject):

    CONFIG = {
        'EVA_LOG_CONFIG': {
            'type': 'string',
            'help': 'Path to logging configuration file',
            'default': '',
        },
        'EVA_PRODUCTSTATUS_URL': {
            'type': 'string',
            'help': 'URL to Productstatus service',
            'default': 'https://productstatus.met.no',
        },
        'EVA_PRODUCTSTATUS_USERNAME': {
            'type': 'string',
            'help': 'Productstatus username for authentication',
            'default': '',
        },
        'EVA_PRODUCTSTATUS_API_KEY': {
            'type': 'string',
            'help': 'Productstatus API key matching the username',
            'default': '',
        },
        'EVA_PRODUCTSTATUS_VERIFY_SSL': {
            'type': 'bool',
            'help': 'Set this option to skip Productstatus SSL certificate verification',
            'default': 'YES',
        },
        'EVA_ADAPTER': {
            'type': 'string',
            'help': 'Python class name of adapters that should be run',
            'default': 'eva.adapter.NullAdapter',
        },
        'EVA_EXECUTOR': {
            'type': 'string',
            'help': 'Python class name of executor that should be used',
            'default': 'eva.executor.ShellExecutor',
        },
        'EVA_LISTENERS': {
            'type': 'list_string',
            'help': 'Comma separated Python class names of listeners that should be run',
            'default': 'eva.listener.RPCListener,eva.listener.ProductstatusListener',
        },
        'EVA_ZOOKEEPER': {
            'type': 'string',
            'help': 'ZooKeeper endpoints in the form <host>:<port>[,<host>:<port>,[...]]/<path>',
            'default': '',
        },
        'EVA_STATSD': {
            'type': 'list_string',
            'help': 'Comma-separated list of StatsD endpoints in the form <host>:<port>',
            'default': '',
        },
        'MARATHON_APP_ID': {
            'type': 'string',
            'help': 'Set by Marathon, and used for Kafka group ID. DO NOT SET THIS VARIABLE MANUALLY!',
            'default': '',
            'hidden': True,
        },
        'MESOS_TASK_ID': {
            'type': 'string',
            'help': 'Set by Marathon, and used for logging purposes. DO NOT SET THIS VARIABLE MANUALLY!',
            'default': '',
            'hidden': True,
        },
    }

    OPTIONAL_CONFIG = [
        'EVA_ADAPTER',
        'EVA_EXECUTOR',
        'EVA_LISTENERS',
        'EVA_LOG_CONFIG',
        'EVA_PRODUCTSTATUS_API_KEY',
        'EVA_PRODUCTSTATUS_URL',
        'EVA_PRODUCTSTATUS_USERNAME',
        'EVA_PRODUCTSTATUS_VERIFY_SSL',
        'EVA_STATSD',
        'EVA_ZOOKEEPER',
        'MARATHON_APP_ID',
        'MESOS_TASK_ID',
    ]

    def __init__(self):
        self.args = None
        self.adapter = None
        self.productstatus_api = None
        self.event_listener = None
        self.env = {}
        self.logger = logging.getLogger('root')
        self.zookeeper = None
        self.listeners = []
        self.adapter = None
        self.executor = None

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

    def setup_environment_variables(self):
        """!
        @brief Read all environment variables from shell.
        """
        self.environment_variables = {key: var for key, var in os.environ.items() if key.startswith(('EVA_', 'MESOS_', 'MARATHON_'))}
        self.env = copy.copy(self.environment_variables)

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
        # Read configuration from file
        if self.env['EVA_LOG_CONFIG']:
            logging.config.fileConfig(self.env['EVA_LOG_CONFIG'], disable_existing_loggers=False)
            self.logger = logging.getLogger('root')

        # Test for Mesos + Marathon execution, and set appropriate log configuration
        if self.env['MARATHON_APP_ID']:
            log_filter = eva.logger.TaskIdLogFilter(
                app_id=self.env['MARATHON_APP_ID'],
                task_id=self.env['MESOS_TASK_ID'],
            )
            # Inject logging filter into any existing loggers so that all
            # output will be correctly filtered.
            self.logger.addFilter(log_filter)
            for key in sorted(logging.Logger.manager.loggerDict.keys()):
                if key == 'root':
                    continue
                logger = logging.Logger.manager.loggerDict[key]
                if not isinstance(logger, logging.Logger):
                    continue
                self.logger.debug('Adding logging filter to logger: %s', key)
                logger.addFilter(log_filter)

        # Disable DEBUG logging on some noisy loggers
        for noisy_logger in NOISY_LOGGERS:
            logging.getLogger(noisy_logger).setLevel(logging.INFO)

    def setup_default_loglevel(self):
        """!
        @brief Ensure that DEBUG loglevel is set if --debug passed to
        arguments, and not using a custom log configuration.
        """
        if self.args.debug:
            self.logger.setLevel(logging.DEBUG)

    def setup_client_group_id(self):
        """!
        @brief Set up Kafka client and group id.

        Use a randomly generated message queue client and group ID, or use the
        name from MARATHON_APP_ID.
        """
        self.client_id = str(uuid.uuid4())
        if self.args.group_id:
            self.group_id = self.args.group_id
        elif self.env['MARATHON_APP_ID']:
            self.group_id = self.env['MARATHON_APP_ID']
        else:
            self.group_id = str(uuid.uuid4())
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
        self.statsd_client = eva.statsd.StatsDClient({'application': self.group_id}, self.env['EVA_STATSD'])
        if len(self.env['EVA_STATSD']) > 0:
            self.logger.info('StatsD client set up with application tag "%s", sending data to: %s', self.group_id, ', '.join(self.env['EVA_STATSD']))
        else:
            self.logger.warning('StatsD not configured, will not send metrics.')

    def setup_zookeeper(self):
        """!
        @brief Instantiate the Zookeeper client, if enabled.
        """
        if not self.env['EVA_ZOOKEEPER']:
            self.logger.warning('ZooKeeper not configured.')
            return

        self.logger.info('Setting up Zookeeper connection to %s', self.env['EVA_ZOOKEEPER'])
        tokens = self.env['EVA_ZOOKEEPER'].strip().split(u'/')
        server_string = tokens[0]
        base_path = os.path.join('/', os.path.join(*tokens[1:]), str(eva.zookeeper_group_id(self.group_id)))
        self.zookeeper = kazoo.client.KazooClient(
            hosts=server_string,
            randomize_hosts=True,
        )
        self.logger.info('Using ZooKeeper, base path "%s"', base_path)
        self.zookeeper.start()
        self.zookeeper.EVA_BASE_PATH = base_path
        self.zookeeper.ensure_path(self.zookeeper.EVA_BASE_PATH)

    def setup_productstatus(self):
        """!
        @brief Instantiate the Productstatus client.
        """
        self.productstatus_api = productstatus.api.Api(
            self.env['EVA_PRODUCTSTATUS_URL'],
            username=self.env['EVA_PRODUCTSTATUS_USERNAME'],
            api_key=self.env['EVA_PRODUCTSTATUS_API_KEY'],
            verify_ssl=self.env['EVA_PRODUCTSTATUS_VERIFY_SSL'],
            timeout=10,
        )

    def setup_listeners(self):
        """!
        @brief Instantiate and configure all message listeners.
        """
        self.listeners = []
        for listener_class in self.env['EVA_LISTENERS']:
            listener = eva.import_module_class(listener_class)(
                self.environment_variables,
                self.logger,
                self.zookeeper,
                client_id=self.client_id,
                group_id=self.group_id,
                productstatus_api=self.productstatus_api,
                statsd=self.statsd_client,
            )
            listener.setup_listener()
            self.logger.info('Adding listener: %s' % listener.__class__)
            self.listeners += [listener]

    def setup_executor(self):
        """!
        @brief Instantiate the configured executor class.
        """
        self.executor = eva.import_module_class(self.env['EVA_EXECUTOR'])(
            self.group_id,
            self.environment_variables,
            self.logger,
            self.zookeeper,
            self.statsd_client,
        )
        self.logger.info('Using executor: %s' % self.executor.__class__)

    def setup_adapter(self):
        """!
        @brief Instantiate the configured adapter class.
        """
        self.adapter = eva.import_module_class(self.env['EVA_ADAPTER'])(
            self.environment_variables,
            self.executor,
            self.productstatus_api,
            self.logger,
            self.zookeeper,
            self.statsd_client,
        )
        self.logger.info('Using adapter: %s' % self.adapter.__class__)

    def setup(self):
        try:
            self.setup_basic_logging()
            self.setup_signals()
            self.setup_environment_variables()
            self.read_configuration()
            self.setup_logging()
            self.parse_args()
            self.setup_default_loglevel()

            self.logger.info('Starting EVA: the EVent Adapter.')
            self.print_environment('Global configuration: ')
            self.setup_client_group_id()
            self.setup_health_check_server()
            self.setup_statsd_client()
            self.setup_zookeeper()
            self.setup_productstatus()
            self.setup_listeners()
            self.setup_executor()
            self.setup_adapter()

        except eva.exceptions.EvaException as e:
            self.logger.critical(str(e))
            self.logger.info('Shutting down EVA due to missing or invalid configuration.')
            sys.exit(1)

        except Exception as e:
            eva.print_exception_as_bug(e, self.logger)
            self.logger.critical('EVA initialization failed. Your code is broken, please fix it.')
            sys.exit(255)

    def start(self):
        try:
            evaloop = eva.eventloop.Eventloop(self.productstatus_api,
                                              self.listeners,
                                              self.adapter,
                                              self.executor,
                                              self.statsd_client,
                                              self.zookeeper,
                                              self.environment_variables,
                                              self.health_check_server,
                                              self.logger,
                                              )

            if self.args.process_all_in_product_instance or self.args.process_data_instance:
                if self.args.process_all_in_product_instance:
                    product_instance = self.productstatus_api.productinstance[self.args.process_all_in_product_instance]
                    evaloop.process_all_in_product_instance(product_instance)
                elif self.args.process_data_instance:
                    evaloop.process_data_instance(self.args.process_data_instance)
                evaloop.sort_queue()
                while evaloop.process_all_events_once():
                    continue
            else:
                evaloop()

        except eva.exceptions.ShutdownException as e:
            self.logger.info(str(e))
        except kazoo.exceptions.ConnectionLoss as e:
            self.logger.critical('Shutting down EVA due to ZooKeeper connection loss: %s', str(e))
            self.statsd.incr('zookeeper_connection_loss')
        except Exception as e:
            eva.print_exception_as_bug(e, self.logger)
            sys.exit(255)

        if self.zookeeper:
            self.logger.info('Stopping ZooKeeper.')
            self.zookeeper.stop()
        self.logger.info('Shutting down EVA.')


if __name__ == "__main__":
    m = Main()
    m.setup()
    m.start()
