# -*- coding: utf-8 -*-

"""Logging configuration test cases."""

import logging

import pytest

from mock import patch
from six.moves import builtins

from rabbithole.cli import configure_logging


@pytest.fixture
def logger():
    """Remove handlers from root logger after each test case."""
    yield
    root_logger = logging.getLogger()
    root_logger.handlers = []


@pytest.mark.usefixtures('logger')
def test_root_level_set_to_debug():
    """Root logger level set to debug."""
    configure_logging(logging.ERROR, None)
    root_logger = logging.getLogger()
    assert root_logger.level == logging.DEBUG


@pytest.mark.usefixtures('logger')
def test_stream_handler_level():
    """Stream handler level set to argument value."""
    expected_value = logging.ERROR
    configure_logging(expected_value, None)
    root_logger = logging.getLogger()
    assert len(root_logger.handlers) == 1
    handler = root_logger.handlers[0]
    assert handler.level == expected_value


@pytest.mark.usefixtures('logger')
def test_file_handler_level():
    """File handler level set to argument value."""
    expected_value = logging.ERROR
    with patch('{}.open'.format(builtins.__name__)):
        configure_logging(expected_value, '<a file>')
    root_logger = logging.getLogger()
    assert len(root_logger.handlers) == 2
    handler = root_logger.handlers[1]
    assert handler.level == expected_value


@pytest.mark.usefixtures('logger')
def test_pika_level_set_warning():
    """Pika logger level is set to warning."""
    configure_logging(logging.DEBUG, None)
    pika_logger = logging.getLogger('pika')
    assert pika_logger.level == logging.WARNING
