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
        exchange = 'exchange'
        payload = 'payload'

        self.batcher.batch_ready = Mock()
        with patch('rabbithole.batcher.threading') as threading:
            self.batcher.message_received_cb('sender', exchange, payload)
            threading.Timer.assert_called_once_with(
                self.batcher.time_limit,
                self.batcher.time_expired_cb,
                (exchange, ),
            )
        self.assertListEqual(self.batcher.batches[exchange], [payload])

    def test_size_limit_exceeded(self):
        """Batch queued when size limit is exceed."""
        exchange = 'exchange'
        payload = 'payload'

        self.batcher.batch_ready = Mock()
        with patch('rabbithole.batcher.threading'):
            for _ in range(self.batcher.size_limit):
                self.batcher.message_received_cb('sender', exchange, payload)

        self.batcher.batch_ready.send.assert_called_with(
            self.batcher,
            exchange_name=exchange,
            batch=[payload] * self.batcher.size_limit,
        )
        self.assertListEqual(self.batcher.batches[exchange], [])

    def test_time_limit_exceeded(self):
        """Batch queued whem time limit is exceeded."""
        exchange = 'exchange'
        payload = 'payload'

        self.batcher.batch_ready = Mock()
        with patch('rabbithole.batcher.threading'):
            self.batcher.message_received_cb('sender', exchange, payload)
        self.batcher.time_expired_cb(exchange)

        self.batcher.batch_ready.send.assert_called_with(
            self.batcher,
            exchange_name=exchange,
            batch=[payload],
        )
        self.assertListEqual(self.batcher.batches[exchange], [])

    def test_expired_timer_not_found(self):
        """Warning written to logs when expired timer is not found."""
        exchange = 'exchange'

        with patch('rabbithole.batcher.LOGGER') as logger:
            self.batcher.time_expired_cb(exchange)
            logger.warning.assert_called_with(
                'Timer not found for: %r',
                exchange,
            )

    def test_timer_already_active(self):
        """Warning written to logs when timer is alreadya active."""
        exchange = 'exchange'

        self.batcher.timers[exchange] = Mock()
        with patch('rabbithole.batcher.LOGGER') as logger:
            self.batcher.start_timer(exchange)
            logger.warning.assert_called_with(
                'Timer already active for: %r',
                exchange,
            )

    def test_cancelled_timer_not_found(self):
        """Warning written to logs when cancelled timer is not found."""
        exchange = 'exchange'

        with patch('rabbithole.batcher.LOGGER') as logger:
            self.batcher.cancel_timer(exchange)
            logger.warning.assert_called_with(
                'Timer not found for: %r',
                exchange,
            )
