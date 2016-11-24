# -*- coding: utf-8 -*-

"""Store messages from rabbitmq into a SQL database."""

import argparse
import logging
import sys

from rabbithole.consumer import Consumer

logger = logging.getLogger(__name__)


def main(argv=None):
    """Console script for rabbithole

    :param argv: Command line arguments
    :type argv: list(str)

    """
    if argv is None:
        argv = sys.argv[1:]

    args = parse_arguments(argv)
    configure_logging(args.log_level)
    consumer = Consumer(args.server, args.exchange_names)

    try:
        consumer.run()
    except KeyboardInterrupt:
        pass


def parse_arguments(argv):
    """Parse command line arguments.

    :returns: Parsed arguments
    :rtype: argparse.Namespace

    """
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        'db_url',
        help='Database connection URL',
    )
    parser.add_argument(
        'insert_query',
        help='Database insert query',
    )
    parser.add_argument(
        'rabbitmq_server',
        help='Rabbitmq server IP address',
    )
    parser.add_argument(
        'exchange_names',
        nargs='+',
        help='Exchange names to bind to',
    )

    log_levels = ['debug', 'info', 'warning', 'error', 'critical']
    parser.add_argument(
        '-l', '--log-level',
        dest='log_level',
        choices=log_levels,
        default='debug',
        help=('Log level. One of {0} or {1} '
              '(%(default)s by default)'
              .format(', '.join(log_levels[:-1]), log_levels[-1])))

    args = parser.parse_args(argv)
    args.log_level = getattr(logging, args.log_level.upper())
    return args


def configure_logging(log_level):
    """Configure logging based on command line argument.

    :param log_level: Log level passed form the command line
    :type log_level: int

    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Log to sys.stderr using log level
    # passed through command line
    log_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    log_handler.setFormatter(formatter)
    log_handler.setLevel(log_level)
    root_logger.addHandler(log_handler)

    # Disable pika extra verbose logging
    logging.getLogger('pika').setLevel(logging.WARNING)


if __name__ == "__main__":
    main()
