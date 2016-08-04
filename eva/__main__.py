import os
import uuid
import signal
import sys
import logging
import logging.config
import argparse
import kazoo.client

import productstatus
import productstatus.api
import productstatus.event

import eva
import eva.statsd
import eva.logger
import eva.eventloop
import eva.adapter
import eva.executor
import eva.rpc_listener


# Environment variables in this list will be censored in the log output.
SECRET_ENVIRONMENT_VARIABLES = [
    'EVA_PRODUCTSTATUS_API_KEY',
]

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


def import_module_class(name):
    components = name.split('.')
    modname = ('.').join(components[0:-1])
    mod = __import__(modname)
    for c in components[1:-1]:
        mod = getattr(mod, c)
    return getattr(mod, components[-1])


def parse_bool(value):
    value = str(value).lower()
    if value == 'yes' or value == 'true' or value == '1':
        return True
    if value == 'no' or value == 'false' or value == '0' or value == 'None':
        return False
    raise ValueError('Invalid boolean value: %s' % value)


def build_argument_list():
    arg = {}

    # Path to logging configuration file
    arg['log_config'] = os.getenv('EVA_LOG_CONFIG')
    # URL to Productstatus service
    arg['productstatus_url'] = os.getenv('EVA_PRODUCTSTATUS_URL', 'https://productstatus.met.no')
    # Productstatus username for authentication
    arg['productstatus_username'] = os.getenv('EVA_PRODUCTSTATUS_USERNAME')
    # Productstatus API key matching the username
    arg['productstatus_api_key'] = os.getenv('EVA_PRODUCTSTATUS_API_KEY')
    # Set this option to skip Productstatus SSL certificate verification
    arg['productstatus_verify_ssl'] = parse_bool(os.getenv('EVA_PRODUCTSTATUS_VERIFY_SSL', True))
    # Python class name of adapters that should be run
    arg['adapter'] = os.getenv('EVA_ADAPTER', 'eva.adapter.NullAdapter')
    # Python class name of executor that should be used
    arg['executor'] = os.getenv('EVA_EXECUTOR', 'eva.executor.ShellExecutor')
    # Comma separated Python class names of listeners that should be run
    arg['listeners'] = os.getenv('EVA_LISTENERS', 'eva.listener.RPCListener,eva.listener.ProductstatusListener')
    # ZooKeeper endpoints
    arg['zookeeper'] = os.getenv('EVA_ZOOKEEPER')
    # StatsD endpoints
    arg['statsd'] = os.getenv('EVA_STATSD', '')

    return arg


if __name__ == "__main__":

    adapter = None
    productstatus_api = None
    event_listener = None
    environment_variables = None

    parser = argparse.ArgumentParser()
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
    args = parser.parse_args()

    try:
        # Catch interrupts and exit cleanly
        def signal_handler(sig, frame):
            raise eva.exceptions.ShutdownException('Caught signal %d, exiting.' % sig)
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        arg = build_argument_list()

        if arg['log_config']:
            logging.config.fileConfig(arg['log_config'])
        else:
            logging.basicConfig(format='%(asctime)s: (%(levelname)s) %(message)s',
                                datefmt='%Y-%m-%dT%H:%M:%S%Z',
                                level=logging.INFO if not args.debug else logging.DEBUG)

        # Randomly generated message queue client and group ID's
        client_id = str(uuid.uuid4())
        group_id = str(uuid.uuid4()) if not args.group_id else args.group_id

        # Extract useful environment variables
        environment_variables = {key: var for key, var in os.environ.items() if key.startswith(('EVA_', 'MARATHON_', 'MESOS_',))}

        # Test for Mesos + Marathon execution, and set appropriate configuration
        logger = logging.getLogger('root')
        if 'MARATHON_APP_ID' in environment_variables:
            group_id = environment_variables['MARATHON_APP_ID']
            log_filter = eva.logger.TaskIdLogFilter(
                app_id=environment_variables['MARATHON_APP_ID'],
                task_id=environment_variables['MESOS_TASK_ID'],
            )
            logger.addFilter(log_filter)
            # Try to hijack the Paramiko logger before it can be instantiated
            # when doing "import paramiko" in the GridEngineExecutor module
            logging.getLogger('paramiko').addFilter(log_filter)

        # Disable DEBUG logging on some noisy loggers
        for noisy_logger in NOISY_LOGGERS:
            logging.getLogger(noisy_logger).setLevel(logging.INFO)

        # Log startup event
        logger.info('Starting EVA: the EVent Adapter.')

        # Print environment variables
        for key, var in sorted(environment_variables.items()):
            if key in SECRET_ENVIRONMENT_VARIABLES:
                var = '****CENSORED****'
            logger.info('Environment: %s=%s' % (key, var))

        # Set up StatsD client
        dsn_list = [x for x in eva.split_comma_separated(arg['statsd']) if len(x) > 0]
        statsd_client = eva.statsd.StatsDClient({'application': group_id}, dsn_list)
        if len(dsn_list) > 0:
            logger.info('StatsD client set up with application tag "%s", sending data to: %s', group_id, ', '.join(dsn_list))
        else:
            logger.warning('StatsD not configured, will not send metrics.')

        # Instantiate the Zookeeper client, if enabled
        if arg['zookeeper']:
            logger.info('Setting up Zookeeper connection to %s', arg['zookeeper'])
            tokens = arg['zookeeper'].strip().split(u'/')
            server_string = tokens[0]
            base_path = os.path.join('/', os.path.join(*tokens[1:]), str(eva.zookeeper_group_id(group_id)))
            zookeeper = kazoo.client.KazooClient(
                hosts=server_string,
                randomize_hosts=True,
            )
            logger.info('Using ZooKeeper, base path "%s"', base_path)
            zookeeper.start()
            zookeeper.EVA_BASE_PATH = base_path
            zookeeper.ensure_path(zookeeper.EVA_BASE_PATH)
        else:
            zookeeper = None
            logger.warning('ZooKeeper not configured.')

        # Instantiate the Productstatus client
        productstatus_api = productstatus.api.Api(
            arg['productstatus_url'],
            username=arg['productstatus_username'],
            api_key=arg['productstatus_api_key'],
            verify_ssl=arg['productstatus_verify_ssl'],
            timeout=10,
        )

        # Instantiate and configure all message listeners
        listeners = []
        listener_classes = eva.split_comma_separated(arg['listeners'])
        for listener_class in listener_classes:
            listener = import_module_class(listener_class)(
                environment_variables,
                logger,
                zookeeper,
                client_id=client_id,
                group_id=group_id,
                productstatus_api=productstatus_api,
                statsd=statsd_client,
            )
            listener.setup_listener()

            #listener.event_listener.json_consumer.poll()
            #listener.event_listener.json_consumer.seek_to_beginning()

            logger.info('Adding listener: %s' % listener.__class__)
            listeners += [listener]

        executor = import_module_class(arg['executor'])(
            group_id,
            environment_variables,
            logger,
            zookeeper,
            statsd_client,
        )
        logger.info('Using executor: %s' % executor.__class__)

        adapter = import_module_class(arg['adapter'])(
            environment_variables,
            executor,
            productstatus_api,
            logger,
            zookeeper,
            statsd_client,
        )
        logger.info('Using adapter: %s' % adapter.__class__)

    except eva.exceptions.EvaException as e:
        logger.critical(str(e))
        logger.info('Shutting down EVA due to missing or invalid configuration.')
        sys.exit(1)

    except Exception as e:
        eva.print_exception_as_bug(e, logger)
        logger.critical('EVA initialization failed. Your code is broken, please fix it.')
        sys.exit(255)

    try:
        evaloop = eva.eventloop.Eventloop(productstatus_api,
                                          listeners,
                                          adapter,
                                          executor,
                                          statsd_client,
                                          environment_variables,
                                          logger,
                                          )
        if args.process_all_in_product_instance:
            product_instance = productstatus_api.productinstance[args.process_all_in_product_instance]
            evaloop.process_all_in_product_instance(product_instance)
            while evaloop.process_all_events_once():
                continue
        elif args.process_data_instance:
            evaloop.process_data_instance(args.process_data_instance)
            while evaloop.process_all_events_once():
                continue
        else:
            evaloop()
    except eva.exceptions.ShutdownException as e:
        logger.info(str(e))
    except Exception as e:
        eva.print_exception_as_bug(e, logger)
        sys.exit(255)

    if zookeeper:
        zookeeper.stop()
    logger.info('Shutting down EVA.')
