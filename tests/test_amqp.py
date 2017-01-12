# -*- coding: utf-8 -*-

"""Message consumer test cases."""

import json

from unittest import TestCase

import blinker

from mock import (
    MagicMock as Mock,
    patch,
)

from rabbithole.amqp import Consumer


class TestConsumer(TestCase):

    """Consumer test cases."""

    def setUp(self):
        """Create fake channel."""
        pika_patcher = patch('rabbithole.amqp.pika')
        pika = pika_patcher.start()
        self.addCleanup(pika_patcher.stop)

        self.channel = pika.BlockingConnection().channel()

    def test_queue_declared(self):
        """Consumer declares a queue on initialization."""
        Consumer('server')
        self.channel.queue_declare.assert_called_with(auto_delete=True)

    def test_exchanges_declared(self):
        """Consumer declares exchanges and returns signal when invoked."""
        exchange = '<exchange>'

        consumer = Consumer('<server>')
        signal = consumer(exchange)

        self.channel.exchange_declare.assert_called_with(
            exchange=exchange,
            exchange_type='fanout',
        )
        self.assertIsInstance(signal, blinker.Signal)

    def test_signal_call_idempotent(self):
        """Same signal is returned when consumer is invoked multiple times."""
        exchange = '<exchange>'

        consumer = Consumer('<server>')
        signal_1 = consumer(exchange)
        signal_2 = consumer(exchange)
        self.assertIs(signal_1, signal_2)

    def test_queue_bound(self):
        """Queue is bound to the exchange."""
        queue = '<queue>'
        exchange = '<exchange>'

        self.channel.queue_declare().method.queue = queue
        consumer = Consumer('<server>')
        consumer(exchange)
        self.channel.queue_bind.assert_called_with(
            exchange=exchange,
            queue=queue,
        )

    def test_run(self):
        """Consumer starts consuming when run is called."""
        exchange = '<exchange>'

        consumer = Consumer('<server>')
        consumer(exchange)
        consumer.run()
        self.channel.start_consuming.assert_called_once_with()

    def test_message_content_type(self):
        """Message is discarded based on content_type."""
        exchange = '<exchange>'
        content_type = 'text/plain'

        consumer = Consumer('<server>')
        consumer(exchange)

        channel = Mock()
        method_frame = Mock()
        header_frame = Mock()
        header_frame.content_type = content_type
        with patch('rabbithole.amqp.LOGGER') as logger:
            consumer.message_received_cb(
                channel,
                method_frame,
                header_frame,
                '<body>',
            )
            logger.warning.assert_called_with(
                'Message discarded. Unexpected content type: %r', content_type)

            channel.basic_nack.assert_called_with(
                method_frame.delivery_tag,
                requeue=False,
            )

    def test_message_decoding_error(self):
        """Warning written to logs when body cannot be decoded."""
        exchange = '<exchange>'
        body = '<body>'

        consumer = Consumer('<server>')
        consumer(exchange)

        consumer.message_received = Mock()
        channel = Mock()
        method_frame = Mock()
        header_frame = Mock()
        header_frame.content_type = 'application/json'

        with patch('rabbithole.amqp.LOGGER') as logger:
            consumer.message_received_cb(
                channel,
                method_frame,
                header_frame,
                body,
            )
            logger.warning.assert_called_once_with(
                'Body decoding error: %r',
                body,
            )

    def test_message_received(self):
        """Message received signal is sent if message is correct."""
        exchange = '<exchange>'
        body = '<body>'

        consumer = Consumer('<server>')
        signal = consumer(exchange)

        def verify(sender, payload):
            """Verify signal is sent as expected."""
            self.assertEqual(sender, consumer)
            self.assertEqual(payload, body)

        signal.connect(verify)

        channel = Mock()
        method_frame = Mock()
        method_frame.exchange = exchange
        header_frame = Mock()
        header_frame.content_type = 'application/json'
        consumer.message_received_cb(
            channel,
            method_frame,
            header_frame,
            json.dumps(body),
        )

        channel.basic_ack.assert_called_with(
            delivery_tag=method_frame.delivery_tag)
