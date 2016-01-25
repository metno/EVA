import os
import sys
import traceback
import argparse
import ConfigParser
import logging
import logging.config

import productstatus
import productstatus.api

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


def add_commandline_arguments(argument_parser):
    argument_parser.add_argument('--log-config', action='store', required=True,
                                 help='Path to logging configuration file')

    # Configuration of Productstatus client.
    argument_parser.add_argument('--productstatus-url', action='store', required=True,
                                 help='URL to Productstatus service')
    argument_parser.add_argument('--productstatus-username', action='store', required=False,
                                 help='Productstatus username for authentication')
    argument_parser.add_argument('--productstatus-api-key', action='store', required=False,
                                 help='Productstatus API key matching the username')
    argument_parser.add_argument('--productstatus-event-socket', action='store', required=True,
                                 help='Socket for receiving Productstatus events')
    argument_parser.add_argument('--productstatus-no-verify-ssl', action='store_true', default=False,
                                 help='Set this option to skip Productstatus SSL certificate verification')

    # Configuration options for EVA about how to run and what jobs to run.
    argument_parser.add_argument('--adapter', action='append', required=False, default=[],
                                 help='Python class name of adapters that should be run; repeat argument for each adapter')
    argument_parser.add_argument('--executor', action='store', required=False, default='eva.executor.NullExecutor',
                                 help='Python class name of executor that should be used')
    argument_parser.add_argument('--checkpoint-file', action='store', required=False,
                                 default='/var/lib/eva/checkpoint.db',
                                 help='Absolute path to EVA checkpoint file.')


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser()

    add_commandline_arguments(argument_parser)

    args = argument_parser.parse_args()
    logging.config.fileConfig(args.log_config)

    logging.info('Starting EVA: the EVent Adapter.')

    config_parser = ConfigParser.SafeConfigParser()
    config_parser.read(args.log_config)

    productstatus_api = productstatus.api.Api(args.productstatus_url,
                                              username=args.productstatus_username,
                                              api_key=args.productstatus_api_key,
                                              verify_ssl=not args.productstatus_no_verify_ssl)

    loop_interval = 10000
    event_listener = productstatus.event.Listener(args.productstatus_event_socket,
                                                  timeout=loop_interval)

    environment_variables = {key: var for key, var in os.environ.iteritems() if key.startswith('EVA_')}
    for key, var in environment_variables.iteritems():
        logging.info('Environment: %s=%s' % (key, var))

    executor = import_module_class(args.executor)(environment_variables)
    logging.info('Using executor: %s' % executor.__class__)

    adapters = []
    for adapter_name in args.adapter:
        adapter_class = import_module_class(adapter_name)
        adapter_instance = adapter_class(productstatus_api, environment_variables)
        logging.info('Adding adapter: %s' % adapter_instance.__class__)
        adapters.append(adapter_instance)

    checkpoint = eva.checkpoint.Checkpoint(args.checkpoint_file)
    logging.info('Checkpoint database: %s' % args.checkpoint_file)

    try:
        evaloop = eva.eventloop.Eventloop(productstatus_api, event_listener, adapters,
                                          executor, checkpoint, environment_variables)
        evaloop.load_state()
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
