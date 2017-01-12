# -*- coding: utf-8 -*-

"""Store messages from an AMQP server into a SQL database."""

import argparse
import logging
import os
import sys
import threading
import time
import traceback

from pprint import pformat

import six
import yaml

from rabbithole.amqp import Consumer
from rabbithole.sql import Database
from rabbithole.batcher import Batcher

LOGGER = logging.getLogger(__name__)
BLOCK_CLASSES = {
    'amqp': Consumer,
    'sql': Database,
}


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
    logging.debug('Configuration:\n%s', pformat(config))

    namespace = {
        block['name']: create_block_instance(block)
        for block in config['blocks']
    }
    batcher_config = {
        'size_limit': config.get('size_limit'),
        'time_limit': config.get('time_limit'),
    }
    for flow in config['flows']:
        create_flow(flow, namespace, batcher_config)
    run_input_blocks(namespace)

    try:
        # Loop needed to be able to catch KeyboardInterrupt
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        LOGGER.info('Interrupted by user')

    return 0


def create_block_instance(block):
    """Create block instance from its configuration

    :param block: Block configuration
    :type block: dict(str)
    :return: Block instance
    :rtype: instance

    """
    block_class = BLOCK_CLASSES[block['type']]

    try:
        block_instance = block_class(
            *block.get('args', []),
            **block.get('kwargs', {})
        )
    except Exception:
        LOGGER.error(traceback.format_exc())
        LOGGER.error(
            'Unable to create %r block: (type: %r, args: %r, kwargs: %r)',
            block['name'],
            block['type'],
            block.get('args', []),
            block.get('kwargs', {}),
        )
        sys.exit(1)

    return block_instance


def create_flow(flow, namespace, batcher_config):
    """Create flow by connecting block signals.

    :param flow: Flow configuration
    :type flow: dict(str)
    :param namespace: Block instances namespace
    :type namespace: dict(str, instance)
    :param batcher_config: Configuration to be passed to batcher objects
    :type batcher_config: dict(str)

    """
    input_block, output_block = flow
    input_block_instance = namespace[input_block['name']]

    try:
        input_signal = input_block_instance(
            *input_block.get('args', []),
            **input_block.get('kwargs', {})
        )
    except Exception:
        LOGGER.error(traceback.format_exc())
        LOGGER.error(
            'Unable to get signal from %r block: (args: %r, kwargs: %r)',
            input_block['name'],
            input_block.get('args', []),
            input_block.get('kwargs', {}),
        )
        sys.exit(1)

    output_block_instance = namespace[output_block['name']]

    try:
        output_cb = output_block_instance(
            *output_block.get('args', []),
            **output_block.get('kwargs', {})
        )
    except Exception:
        LOGGER.error(traceback.format_exc())
        LOGGER.error(
            'Unable to get callback from %r block: (args: %r, kwargs: %r)',
            output_block['name'],
            output_block.get('args', []),
            output_block.get('kwargs', {}),
        )
        sys.exit(1)

    batcher = Batcher(**batcher_config)
    input_signal.connect(batcher.message_received_cb, weak=False)
    batcher.batch_ready.connect(output_cb, weak=False)


def run_input_blocks(namespace):
    """Run inputs blocks and start receiving messages from them.

    :param namespace: Block instances namespace
    :type namespace: dict(str, instance)

    """
    for block_name, block_instance in six.iteritems(namespace):
        run_method = getattr(block_instance, 'run', None)
        if run_method:
            thread = threading.Thread(name=block_name, target=run_method)
            thread.daemon = True
            thread.start()


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
