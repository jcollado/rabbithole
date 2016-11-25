# -*- coding: utf-8 -*-

"""Store messages from rabbitmq into a SQL database."""

import argparse
import logging
import os
import sys

import pika
import sqlalchemy
import yaml

from rabbithole.consumer import Consumer
from rabbithole.db import Database
from rabbithole.batcher import Batcher

LOGGER = logging.getLogger(__name__)


def main(argv=None):
    """Console script for rabbithole

    :param argv: Command line arguments
    :type argv: list(str)

    """
    if argv is None:
        argv = sys.argv[1:]

    args = parse_arguments(argv)
    config = args.config
    configure_logging(args.log_level)

    try:
        consumer = Consumer(config['rabbitmq'], config['output'].keys())
    except pika.exceptions.AMQPError as exception:
        LOGGER.error('Rabbitmq connectivity error: %s', exception)
        return 1

    try:
        database = Database(config['database'], config['output']).connect()
    except sqlalchemy.exc.SQLAlchemyError as exception:
        LOGGER.error(exception)
        return 1

    batcher = Batcher(database)
    consumer.message_received.connect(batcher.message_received_cb)

    try:
        consumer.run()
    except KeyboardInterrupt:
        LOGGER.info('Interrupted by user')

    return 0


def parse_arguments(argv):
    """Parse command line arguments.

    :returns: Parsed arguments
    :rtype: argparse.Namespace

    """
    parser = argparse.ArgumentParser(description=__doc__)

    def yaml_file(path):
        """Yaml file argument.

        :param path: Path to the yaml file
        :type path: str

        """
        if not os.path.isfile(path):
            raise argparse.ArgumentTypeError('File not found')

        with open(path) as file_:
            try:
                data = yaml.load(file_)
            except yaml.YAMLError:
                raise argparse.ArgumentTypeError('YAML parsing error')

        return data

    parser.add_argument(
        'config',
        type=yaml_file,
        help='Configuration file',
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
    formatter = logging.Formatter(
        '%(asctime)s %(threadName)s %(levelname)s: %(message)s')
    log_handler.setFormatter(formatter)
    log_handler.setLevel(log_level)
    root_logger.addHandler(log_handler)

    # Disable pika extra verbose logging
    logging.getLogger('pika').setLevel(logging.WARNING)


if __name__ == "__main__":
    sys.exit(main())
