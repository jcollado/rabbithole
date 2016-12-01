# -*- coding: utf-8 -*-

"""CLI test cases."""

import argparse
import logging

from unittest import TestCase

import pika
import sqlalchemy
import yaml

from mock import patch
from six import StringIO

from rabbithole.cli import (
    configure_logging,
    main,
    parse_arguments,
)


class TestMain(TestCase):

    """Main entry point test cases."""

    def setUp(self):
        """Patch parse_arguments function."""
        parse_arguments_patcher = patch('rabbithole.cli.parse_arguments')
        parse_arguments_ = parse_arguments_patcher.start()
        parse_arguments_.return_value = argparse.Namespace(
            config={
                'amqp': '<amqp server>',
                'database': '<database url>',
                'output': {
                    'exchange#1': 'query#1',
                    'exchange#2': 'query#2',
                },
            },
            log_level=logging.CRITICAL,
        )
        self.addCleanup(parse_arguments_patcher.stop)

    def test_exit_on_amqp_error(self):
        """Exit when there's an AMQP connectivity error."""
        with patch('rabbithole.cli.Consumer') as consumer_cls:
            consumer_cls.side_effect = pika.exceptions.AMQPError
            return_code = main()
            self.assertEqual(return_code, 1)

    def test_exit_on_database_error(self):
        """Exit when there's a database connectivity error."""
        with patch('rabbithole.cli.Consumer'), \
                patch('rabbithole.cli.Database') as database_cls:
            database_cls.side_effect = sqlalchemy.exc.SQLAlchemyError
            return_code = main()
            self.assertEqual(return_code, 1)

    def test_signals_connected(self):
        """Signals are connected as expected."""
        with patch('rabbithole.cli.Consumer') as consumer_cls, \
                patch('rabbithole.cli.Database') as database_cls, \
                patch('rabbithole.cli.Batcher') as batcher_cls:
            return_code = main()

            consumer_cls().message_received.connect.assert_called_once_with(
                batcher_cls().message_received_cb)
            batcher_cls().batch_ready.connect.assert_called_once_with(
                database_cls().connect().batch_ready_cb)

            self.assertEqual(return_code, 0)

    def test_exit_on_keyboard_interrupt(self):
        """Exit when user hits Ctrl+C."""
        with patch('rabbithole.cli.Consumer') as consumer_cls, \
                patch('rabbithole.cli.Database'), \
                patch('rabbithole.cli.Batcher'):
            consumer_cls().run.side_effect = KeyboardInterrupt
            return_code = main()
            self.assertEqual(return_code, 0)


class TestParseArguments(TestCase):

    """Argument parsing test cases."""

    def test_config_file_does_not_exist(self):
        """SystemExit is raised if the configuration file does not exist."""
        # Do not include error output in test output
        with self.assertRaises(SystemExit), patch('rabbithole.cli.sys.stderr'):
            parse_arguments(['file-does-not-exist'])

    def test_config_file_invalid(self):
        """SystemExit is raised if the configuration file is invalid."""
        with self.assertRaises(SystemExit), \
                patch('rabbithole.cli.sys.stderr'), \
                patch('rabbithole.cli.os') as os_, \
                patch('rabbithole.cli.open') as open_:
            os_.path.isfile.return_value = True
            open_().__enter__.return_value = StringIO('>invalid yaml<')
            parse_arguments(['some file'])

    def test_config_file_load_success(self):
        """Config file successfully loaded."""
        expected_value = {'a': 'value'}
        with patch('rabbithole.cli.os') as os_, \
                patch('rabbithole.cli.open') as open_:
            os_.path.isfile.return_value = True
            open_().__enter__.return_value = (
                StringIO(yaml.dump(expected_value)))
            args = parse_arguments(['some file'])

        self.assertDictEqual(args.config, expected_value)


class TestConfigureLogging(TestCase):

    """Logging configuration test cases."""

    def tearDown(self):
        """Delete root logger handlers."""
        logging.getLogger().handlers = []

    def test_root_level_set_to_debug(self):
        """Root logger level set to debug."""
        configure_logging(logging.ERROR)
        root_logger = logging.getLogger()
        self.assertEqual(root_logger.level, logging.DEBUG)

    def test_stream_handler_level(self):
        """Stream handler level set to argument value."""
        expected_value = logging.ERROR
        configure_logging(expected_value)
        root_logger = logging.getLogger()
        self.assertEqual(len(root_logger.handlers), 1)
        handler = root_logger.handlers[0]
        self.assertEqual(handler.level, expected_value)

    def test_pika_level_set_warning(self):
        """Pika logger level is set to warning."""
        configure_logging(logging.DEBUG)
        pika_logger = logging.getLogger('pika')
        self.assertEqual(pika_logger.level, logging.WARNING)
