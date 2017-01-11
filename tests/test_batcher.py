# -*- coding: utf-8 -*-

"""Batcher test cases."""

from unittest import TestCase

from mock import (
    MagicMock as Mock,
    patch,
)
from six.moves import range

from rabbithole.batcher import Batcher


class TestBatcher(TestCase):

    """Batcher test cases."""

    def setUp(self):
        """Create batcher object."""
        size_limit = 5
        time_limit = 15
        self.batcher = Batcher(size_limit, time_limit)

    def test_first_message_received(self):
        """Message appended to batch and timer started."""
        payload = 'payload'

        self.batcher.batch_ready = Mock()
        with patch('rabbithole.batcher.threading') as threading:
            self.batcher.message_received_cb('sender', payload)
            threading.Timer.assert_called_once_with(
                self.batcher.time_limit,
                self.batcher.time_expired_cb,
            )
        self.assertListEqual(self.batcher.batch, [payload])

    def test_size_limit_exceeded(self):
        """Batch queued when size limit is exceed."""
        payload = 'payload'

        self.batcher.batch_ready = Mock()
        with patch('rabbithole.batcher.threading'):
            for _ in range(self.batcher.size_limit):
                self.batcher.message_received_cb('sender', payload)

        self.batcher.batch_ready.send.assert_called_with(
            self.batcher,
            batch=[payload] * self.batcher.size_limit,
        )
        self.assertListEqual(self.batcher.batch, [])

    def test_time_limit_exceeded(self):
        """Batch queued when time limit is exceeded."""
        payload = 'payload'

        self.batcher.batch_ready = Mock()
        with patch('rabbithole.batcher.threading'):
            self.batcher.message_received_cb('sender', payload)
        self.batcher.time_expired_cb()

        self.batcher.batch_ready.send.assert_called_with(
            self.batcher,
            batch=[payload],
        )
        self.assertListEqual(self.batcher.batch, [])

    def test_expired_timer_not_active(self):
        """Warning written to logs when expired timer is not active."""
        with patch('rabbithole.batcher.LOGGER') as logger:
            self.batcher.time_expired_cb()
            logger.warning.assert_called_with('Timer is not active')

    def test_timer_already_active(self):
        """Warning written to logs when timer is already active."""
        self.batcher.timer = Mock()
        with patch('rabbithole.batcher.LOGGER') as logger:
            self.batcher.start_timer()
            logger.warning.assert_called_with('Timer already active')

    def test_cancelled_timer_not_active(self):
        """Warning written to logs when cancelled timer is not active."""
        with patch('rabbithole.batcher.LOGGER') as logger:
            self.batcher.cancel_timer()
            logger.warning.assert_called_with('Timer is not active')
