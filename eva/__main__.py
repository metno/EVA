import os
import sys
import traceback
import ConfigParser
import logging
import logging.config

import productstatus
import productstatus.api
import productstatus.event

import eva.eventloop
import eva.adapter
import eva.executor


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


if __name__ == "__main__":

    adapter = None
    productstatus_api = None
    event_listener = None
    environment_variables = None

    try:
        arg = build_argument_list()

        if arg['log_config']:
            logging.config.fileConfig(arg['log_config'])
        else:
            logging.basicConfig(format='%(asctime)s: (%(levelname)s) %(message)s',
                                datefmt='%Y-%m-%dT%H:%M:%S%Z',
                                level=logging.DEBUG)

        logging.info('Starting EVA: the EVent Adapter.')

        config_parser = ConfigParser.SafeConfigParser()
        config_parser.read(arg['log_config'])

        productstatus_api = productstatus.api.Api(arg['productstatus_url'],
                                                  username=arg['productstatus_username'],
                                                  api_key=arg['productstatus_api_key'],
                                                  verify_ssl=arg['productstatus_verify_ssl'])

        event_listener = productstatus.event.Listener(arg['productstatus_event_socket'])

        environment_variables = {key: var for key, var in os.environ.iteritems() if key.startswith('EVA_')}
        for key, var in sorted(environment_variables.iteritems()):
            logging.info('Environment: %s=%s' % (key, var))

        executor = import_module_class(arg['executor'])(environment_variables)
        logging.info('Using executor: %s' % executor.__class__)

        adapter = import_module_class(arg['adapter'])(
            environment_variables,
            executor,
            productstatus_api,
        )
        logging.info('Using adapter: %s' % adapter.__class__)

    except eva.exceptions.MissingConfigurationException, e:
        logging.critical(e)
        logging.info('Shutting down because of missing configuration.')
        sys.exit(1)

    try:
        evaloop = eva.eventloop.Eventloop(productstatus_api,
                                          event_listener,
                                          adapter,
                                          environment_variables
                                          )
        evaloop()
    except KeyboardInterrupt:
        pass
    except Exception, e:
        logging.critical("Fatal error: %s" % e)
        exception = traceback.format_exc().split("\n")
        logging.debug("***********************************************************")
        logging.debug("Uncaught exception during program execution. THIS IS A BUG!")
        logging.debug("***********************************************************")
        for line in exception:
            logging.debug(line)
        sys.exit(255)

    logging.info('Shutting down EVA.')
