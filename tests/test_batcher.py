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

    def test_first_message_received(self):
        """Message appended to batch and timer started."""
        exchange = 'exchange'
        payload = 'payload'

        batcher = Batcher()
        batcher.batch_ready = Mock()
        with patch('rabbithole.batcher.threading') as threading:
            batcher.message_received_cb('sender', exchange, payload)
            threading.Timer.assert_called_once_with(
                batcher.TIME_LIMIT,
                batcher.time_expired_cb,
                (exchange, ),
            )
        self.assertListEqual(batcher.batches[exchange], [payload])

    def test_size_limit_exceeded(self):
        """Batch queued when size limit is exceed."""
        exchange = 'exchange'
        payload = 'payload'

        batcher = Batcher()
        batcher.batch_ready = Mock()
        with patch('rabbithole.batcher.threading'):
            for _ in range(batcher.SIZE_LIMIT):
                batcher.message_received_cb('sender', exchange, payload)

        batcher.batch_ready.send.assert_called_with(
            batcher,
            exchange_name=exchange,
            batch=[payload] * batcher.SIZE_LIMIT,
        )
        self.assertListEqual(batcher.batches[exchange], [])

    def test_time_limit_exceeded(self):
        """Batch queued whem time limit is exceeded."""
        exchange = 'exchange'
        payload = 'payload'

        batcher = Batcher()
        batcher.batch_ready = Mock()
        with patch('rabbithole.batcher.threading'):
            batcher.message_received_cb('sender', exchange, payload)
        batcher.time_expired_cb(exchange)

        batcher.batch_ready.send.assert_called_with(
            batcher,
            exchange_name=exchange,
            batch=[payload],
        )
        self.assertListEqual(batcher.batches[exchange], [])

    def test_expired_timer_not_found(self):
        """Warning written to logs when expired timer is not found."""
        exchange = 'exchange'

        batcher = Batcher()
        with patch('rabbithole.batcher.LOGGER') as logger:
            batcher.time_expired_cb(exchange)
            logger.warning.assert_called_with(
                'Timer not found for: %r',
                exchange,
            )

    def test_timer_already_active(self):
        """Warning written to logs when timer is alreadya active."""
        exchange = 'exchange'

        batcher = Batcher()
        batcher.timers[exchange] = Mock()
        with patch('rabbithole.batcher.LOGGER') as logger:
            batcher.start_timer(exchange)
            logger.warning.assert_called_with(
                'Timer already active for: %r',
                exchange,
            )

    def test_cancelled_timer_not_found(self):
        """Warning written to logs when cancelled timer is not found."""
        exchange = 'exchange'

        batcher = Batcher()
        with patch('rabbithole.batcher.LOGGER') as logger:
            batcher.cancel_timer(exchange)
            logger.warning.assert_called_with(
                'Timer not found for: %r',
                exchange,
            )
