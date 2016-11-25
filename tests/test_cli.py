# -*- coding: utf-8 -*-

"""CLI test cases."""

import logging

from unittest import TestCase

import yaml

from mock import patch
from six import StringIO

from rabbithole.cli import (
    configure_logging,
    parse_arguments,
)


class TestParseArguments(TestCase):

    """Argument parsing test cases."""

    def test_systemexit_raised_on_config_file_does_not_exist(self):
        """SystemExit is raised if the configuration file does not exist."""
        # Do not include error output in test output
        with self.assertRaises(SystemExit), patch('rabbithole.cli.sys.stderr'):
            parse_arguments(['file-does-not-exist'])

    def test_systemexit_raised_on_config_file_invalid(self):
        """SystemExit is raised if the configuration file is invalid."""
        with self.assertRaises(SystemExit), \
                patch('rabbithole.cli.sys.stderr'), \
                patch('rabbithole.cli.os') as os, \
                patch('rabbithole.cli.open') as open:
            os.path.isfile.return_value = True
            open().__enter__.return_value = StringIO('>invalid yaml<')
            parse_arguments(['some file'])

    def test_config_file_load_success(self):
        """Config file successfully loaded."""
        expected_value = {'a': 'value'}
        with patch('rabbithole.cli.os') as os, \
                patch('rabbithole.cli.open') as open:
            os.path.isfile.return_value = True
            open().__enter__.return_value = StringIO(yaml.dump(expected_value))
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

    def test_streamh_handler_level_set_to_argument(self):
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
