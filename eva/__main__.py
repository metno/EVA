
import argparse
import ConfigParser
import logging
import logging.config
import sys

import eva

if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument('--config', action='store', required=True,
                                 help='Path to configuration file')
    args = argument_parser.parse_args()
    logging.config.fileConfig(args.config)

    config_parser = ConfigParser.SafeConfigParser()
    config_parser.read(args.config)

    kwargs = {
        'productstatus_url': config_parser.get('productstatus', 'url'),
        'productstatus_username': config_parser.get('productstatus', 'username'),
        'productstatus_api_key': config_parser.get('productstatus', 'api_key'),
        'productstatus_verify_ssl': config_parser.getboolean('productstatus', 'verify_ssl'),
        'productstatus_zmq_sub': config_parser.get('productstatus', 'zeromq_subscribe_socket'),
    }

    filters = []
    for fk, fv in config_parser.items('event_filters'):
        op, val = [x.strip() for x in fv.split(' ', 1)]   # FIXME whitespace problems in value
        filters.append((fk, op, val))
    fop = eva.FilterOperatorHandler(filters, eva.DumpHandler())

    kwargs['handler'] = fop

    try:
        ef = eva.Listener(**kwargs)
        ef.listen()
    except KeyboardInterrupt:
        pass
    except Exception, e:
        logging.critical("Fatal error: %s" % e)
        logging.debug("Fatal exception is:", exc_info=True)
        sys.exit(255)
