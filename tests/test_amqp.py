# -*- coding: utf-8 -*-

"""Message consumer test cases."""

import json

from unittest import TestCase

from mock import (
    MagicMock as Mock,
    patch,
)

from rabbithole.amqp import Consumer


class TestConsumer(TestCase):

    """Consumer test cases."""

    QUEUE = '<queue_name>'
    EXCHANGES = [
        'exchange#1',
        'exchange#2',
    ]

    def setUp(self):
        """Create fake channel."""
        pika_patcher = patch('rabbithole.amqp.pika')
        pika = pika_patcher.start()
        self.addCleanup(pika_patcher.stop)

        self.channel = pika.BlockingConnection().channel()

    def test_exchanges_declared(self):
        """Consumer declares exhanges on initialization"""
        Consumer('<server>', self.EXCHANGES)
        for exchange in self.EXCHANGES:
            self.channel.exchange_declare.assert_any_call(
                exchange=exchange,
                exchange_type='fanout',
            )

    def test_queue_bound(self):
        """Queue is bound to the exchanges."""
        self.channel.queue_declare().method.queue = self.QUEUE
        Consumer('<server>', self.EXCHANGES)
        for exchange in self.EXCHANGES:
            self.channel.queue_bind.assert_any_call(
                exchange=exchange,
                queue=self.QUEUE,
            )

    def test_run(self):
        """Consumer starts consuming when run is called."""
        consumer = Consumer('<server>', self.EXCHANGES)
        consumer.run()
        self.channel.start_consuming.assert_called_once_with()

    def test_message_content_type(self):
        """Message is discarded based on content_type."""
        consumer = Consumer('<server>', self.EXCHANGES)

        channel = Mock()
        method_frame = Mock()
        header_frame = Mock()
        header_frame.content_type = 'text/plain'
        consumer.message_received_cb(
            channel,
            method_frame,
            header_frame,
            '<body>',
        )

        channel.basic_nack.assert_called_with(
            method_frame.delivery_tag,
            requeue=False,
        )

    def test_message_decoding_error(self):
        """Warning written to logs when body cannot be decoded."""
        body = '<body>'
        consumer = Consumer('<server>', self.EXCHANGES)

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
        body = '<body>'
        consumer = Consumer('<server>', self.EXCHANGES)

        consumer.message_received = Mock()
        channel = Mock()
        method_frame = Mock()
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
        consumer.message_received.send.assert_called_with(
            consumer,
            exchange_name=method_frame.exchange,
            payload=body,
        )
