# -*- coding: utf-8 -*-

"""CLI test cases."""

import logging

from unittest import TestCase

from rabbithole.cli import configure_logging


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
