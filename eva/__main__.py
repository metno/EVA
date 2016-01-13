
import argparse
import ConfigParser
import logging
import logging.config
import sys

import productstatus
import eva.eventloop
import eva.adapter
import eva.executor


def add_commandline_arguments(argument_parser):
    argument_parser.add_argument('--log-config', action='store', required=True,
                                 help='Path to logging configuration file')

    # Configuration of Productstatus client.
    argument_parser.add_argument('--productstatus-url', action='store', required=True,
                                 help='URL to Productstatus service.')
    argument_parser.add_argument('--productstatus-username', action='store', required=True,
                                 help='Productstatus username for authentication')
    argument_parser.add_argument('--productstatus-api-key', action='store', required=True,
                                 help='Productstatus api key issued for a specific user')
    argument_parser.add_argument('--productstatus-zeromq-subscribe-socket', action='store', required=True,
                                 help='ZeroMQ socket for receiving productstatus events')
    argument_parser.add_argument('--productstatus-verify-ssl', action='store_true', default=False,
                                 help='Verify SSL. Set to False if omitted.')

    # Configuration options for EVA about how to run and what jobs to run.
    argument_parser.add_argument('--adapter', action='append', required=True,
                                 help='Name of adapter that should be run. Repeat argument for each adapter.')
    argument_parser.add_argument('--executor', action='store', required=False, default='DummyExecutor',
                                 help='Executor class for EVA')

if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser()

    add_commandline_arguments(argument_parser)

    args = argument_parser.parse_args()
    logging.config.fileConfig(args.log_config)

    config_parser = ConfigParser.SafeConfigParser()
    config_parser.read(args.log_config)

    productstatus_api = productstatus.api.Api(args.productstatus_url,
                                              username=args.productstatus_username,
                                              api_key=args.productstatus_api_key,
                                              verify_ssl=args.productstatus_verify_ssl)

    loop_interval = 10000
    event_listener = productstatus.event.Listener(args.productstatus_zeromq_subscribe_socket,
                                                  timeout=loop_interval)

    adapters = []
    for adapter_name in args.adapter:
        adapter_class = getattr(eva.adapter, adapter_name)
        adapters.append(adapter_class(productstatus_api))

    executor = getattr(eva.executor, args.executor)()

    try:
        evaloop = eva.eventloop.Eventloop(productstatus_api, event_listener, adapters, executor)
        evaloop()
    except Exception, e:
        logging.critical("Fatal error: %s" % e)
        logging.debug("Fatal exception is:", exc_info=True)
        sys.exit(255)
