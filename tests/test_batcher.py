# -*- coding: utf-8 -*-

"""Batcher test cases."""

import pytest

from mock import (
    MagicMock as Mock,
    patch,
)
from six.moves import range  # pylint:disable=redefined-builtin

from rabbithole.batcher import Batcher


@pytest.fixture(name='batcher')
def fixture_batcher():
    """Create a batcher instance."""
    size_limit = 5
    time_limit = 15
    batcher = Batcher(size_limit, time_limit)
    return batcher


def test_first_message_received(batcher):
    """Message appended to batch and timer started."""
    payload = 'payload'

    batcher.batch_ready = Mock()
    with patch('rabbithole.batcher.threading') as threading:
        batcher.message_received_cb('sender', payload)
        threading.Timer.assert_called_once_with(
            batcher.time_limit,
            batcher.time_expired_cb,
        )
    assert batcher.batch == [payload]


def test_size_limit_exceeded(batcher):
    """Batch queued when size limit is exceed."""
    payload = 'payload'

    batcher.batch_ready = Mock()
    with patch('rabbithole.batcher.threading'):
        for _ in range(batcher.size_limit):
            batcher.message_received_cb('sender', payload)

    batcher.batch_ready.send.assert_called_with(
        batcher,
        batch=[payload] * batcher.size_limit,
    )
    assert batcher.batch == []


def test_time_limit_exceeded(batcher):
    """Batch queued when time limit is exceeded."""
    payload = 'payload'

    batcher.batch_ready = Mock()
    with patch('rabbithole.batcher.threading'):
        batcher.message_received_cb('sender', payload)
    batcher.time_expired_cb()

    batcher.batch_ready.send.assert_called_with(batcher, batch=[payload])
    assert batcher.batch == []


def test_empty_batch(batcher):
    """Warning written to logs when batch is empty."""
    with patch('rabbithole.batcher.LOGGER') as logger:
        batcher.queue_batch()
        logger.warning.assert_called_with('[%x] Nothing to queue', id(batcher))


def test_expired_timer_not_active(batcher):
    """Warning written to logs when expired timer is not active."""
    with patch('rabbithole.batcher.LOGGER') as logger:
        batcher.time_expired_cb()
        logger.warning.assert_called_with(
            '[%x] Timer is not active', id(batcher))


def test_timer_already_active(batcher):
    """Warning written to logs when timer is already active."""
    batcher.timer = Mock()
    with patch('rabbithole.batcher.LOGGER') as logger:
        batcher.start_timer()
        logger.warning.assert_called_with(
            '[%x] Timer already active', id(batcher))


def test_cancelled_timer_not_active(batcher):
    """Warning written to logs when cancelled timer is not active."""
    with patch('rabbithole.batcher.LOGGER') as logger:
        batcher.cancel_timer()
        logger.warning.assert_called_with(
            '[%x] Timer is not active', id(batcher))
