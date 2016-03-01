import os
import sys
import traceback
import logging
import logging.config
import argparse
import json

import productstatus
import productstatus.api
import productstatus.event

import eva.eventloop
import eva.adapter
import eva.executor


# Environment variables in this list will be censored in the log output.
SECRET_ENVIRONMENT_VARIABLES = [
    'EVA_PRODUCTSTATUS_API_KEY',
]


def import_module_class(name):
    components = name.split('.')
    modname = ('.').join(components[0:-1])
    mod = __import__(modname)
    for c in components[1:-1]:
        mod = getattr(mod, c)
    return getattr(mod, components[-1])


def parse_bool(value):
    value = unicode(value).lower()
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
    # Socket for receiving Productstatus events
    arg['productstatus_event_socket'] = os.getenv('EVA_PRODUCTSTATUS_EVENT_SOCKET', 'tcp://productstatus.met.no:9797')
    # Set this option to skip Productstatus SSL certificate verification
    arg['productstatus_verify_ssl'] = parse_bool(os.getenv('EVA_PRODUCTSTATUS_VERIFY_SSL', True))
    # Comma_separated Python class name of adapters that should be run
    arg['adapter'] = os.getenv('EVA_ADAPTER', 'eva.adapter.NullAdapter')
    # Python class name of executor that should be used
    arg['executor'] = os.getenv('EVA_EXECUTOR', 'eva.executor.ShellExecutor')

    return arg


class MesosLogAdapter(logging.LoggerAdapter):
    """
    @brief Log extra This example adapter expects the passed in dict-like object to have a
    'connid' key, whose value in brackets is prepended to the log message.
    """
    def process(self, msg, kwargs):
        return '%s %s %s' % (self.extra['MARATHON_APP_ID'], self.extra['MESOS_TASK_ID'], msg), kwargs


if __name__ == "__main__":

    adapter = None
    productstatus_api = None
    event_listener = None
    environment_variables = None

    parser = argparse.ArgumentParser()
    parser.add_argument('--oneshot',
                        action='store',
                        type=unicode,
                        required=False,
                        help='Process all DataInstance resources belonging to a specific ProductInstance')
    parser.add_argument('--mesos-log',
                        action='store_true',
                        default=False,
                        help='Use this flag if running inside a Mesos Docker container for extra logging output')
    args = parser.parse_args()

    try:
        arg = build_argument_list()

        if arg['log_config']:
            logging.config.fileConfig(arg['log_config'])
        else:
            logging.basicConfig(format='%(asctime)s: (%(levelname)s) %(message)s',
                                datefmt='%Y-%m-%dT%H:%M:%S%Z',
                                level=logging.DEBUG)

        # Extract useful environment variables
        environment_variables = {key: var for key, var in os.environ.iteritems() if key.startswith(('EVA_', 'MARATHON_', 'MESOS_',))}

        # Test for Mesos + Marathon execution, and set appropriate logging configuration
        logger = logging.getLogger('root')
        if 'MARATHON_APP_ID' in environment_variables:
            logger = MesosLogAdapter(logger, environment_variables)

        logger.info('Starting EVA: the EVent Adapter.')

        for key, var in sorted(environment_variables.iteritems()):
            if key in SECRET_ENVIRONMENT_VARIABLES:
                var = '****CENSORED****'
            logger.debug('Environment: %s=%s' % (key, var))

        productstatus_api = productstatus.api.Api(arg['productstatus_url'],
                                                  username=arg['productstatus_username'],
                                                  api_key=arg['productstatus_api_key'],
                                                  verify_ssl=arg['productstatus_verify_ssl'],
                                                  timeout=10)

        event_listener = productstatus.event.Listener(arg['productstatus_event_socket'])

        executor = import_module_class(arg['executor'])(environment_variables, logger)
        logger.debug('Using executor: %s' % executor.__class__)

        adapter = import_module_class(arg['adapter'])(
            environment_variables,
            executor,
            productstatus_api,
            logger,
        )
        logger.debug('Using adapter: %s' % adapter.__class__)

    except eva.exceptions.EvaException, e:
        logger.critical(unicode(e))
        logger.info('Shutting down due to missing or invalid configuration.')
        sys.exit(1)

    try:
        evaloop = eva.eventloop.Eventloop(productstatus_api,
                                          event_listener,
                                          adapter,
                                          environment_variables,
                                          logger,
                                          )
        if args.oneshot:
            product_instance = productstatus_api.productinstance[args.oneshot]
            evaloop.process_all_in_product_instance(product_instance)
        else:
            evaloop()
    except KeyboardInterrupt:
        pass
    except Exception, e:
        logger.critical("Fatal error: %s" % e)
        exception = traceback.format_exc().split("\n")
        logger.debug("***********************************************************")
        logger.debug("Uncaught exception during program execution. THIS IS A BUG!")
        logger.debug("***********************************************************")
        for line in exception:
            logger.debug(line)
        sys.exit(255)

    logger.info('Shutting down EVA.')
