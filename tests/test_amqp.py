# -*- coding: utf-8 -*-

"""AMQP input block test cases."""

import json

import blinker
import pytest

from mock import (
    MagicMock as Mock,
    patch,
)

from rabbithole.amqp import Consumer


@pytest.fixture(name='pika')
def fixture_pika():
    """Patch the pika module."""
    pika_patcher = patch('rabbithole.amqp.pika')
    pika = pika_patcher.start()
    yield pika
    pika_patcher.stop()


@pytest.fixture(name='channel')
def fixture_channel(pika):
    """Create a mock channel."""
    channel = pika.BlockingConnection().channel()
    return channel


def test_queue_declared(channel):
    """Consumer declares a queue on initialization."""
    Consumer('server')
    channel.queue_declare.assert_called_with(auto_delete=True)


def test_exchanges_declared(channel):
    """Consumer declares exchanges and returns signal when invoked."""
    exchange = '<exchange>'

    consumer = Consumer('<server>')
    signal = consumer(exchange)

    channel.exchange_declare.assert_called_with(exchange=exchange)
    assert isinstance(signal, blinker.Signal)


@pytest.mark.usefixtures('pika')
def test_signal_call_idempotent():
    """Same signal is returned when consumer is invoked multiple times."""
    exchange = '<exchange>'

    consumer = Consumer('<server>')
    signal_1 = consumer(exchange)
    signal_2 = consumer(exchange)
    assert signal_1 is signal_2


def test_queue_bound(channel):
    """Queue is bound to the exchange."""
    queue = '<queue>'
    exchange = '<exchange>'

    channel.queue_declare().method.queue = queue
    consumer = Consumer('<server>')
    consumer(exchange)
    channel.queue_bind.assert_called_with(
        exchange=exchange,
        queue=queue,
    )


def test_run(channel):
    """Consumer starts consuming when run is called."""
    exchange = '<exchange>'

    consumer = Consumer('<server>')
    consumer(exchange)
    consumer.run()
    channel.start_consuming.assert_called_once_with()


@pytest.mark.usefixtures('pika')
def test_message_content_type():
    """Message discarded when content type is not valid json."""
    exchange = '<exchange>'
    content_type = 'text/plain'

    consumer = Consumer('<server>')
    consumer(exchange)

    channel = Mock()
    method_frame = Mock()
    header_frame = Mock()
    header_frame.content_type = content_type
    with patch('rabbithole.amqp.LOGGER') as logger:
        body = '<body>'
        consumer.message_received_cb(
            channel,
            method_frame,
            header_frame,
            body,
        )
        logger.warning.assert_any_call(
            'Unexpected content type: %r', content_type)
        logger.warning.assert_any_call('Body decoding error: %r', body)
        channel.basic_nack.assert_called_with(
            method_frame.delivery_tag,
            requeue=False,
        )


@pytest.mark.usefixtures('pika')
def test_message_decoding_error():
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


@pytest.mark.usefixtures('pika')
def test_message_received():
    """Message received signal is sent if message is correct."""
    exchange = '<exchange>'
    body = '<body>'

    consumer = Consumer('<server>')
    signal = consumer(exchange)

    def verify(sender, payload):
        """Verify signal is sent as expected."""
        assert sender == consumer
        assert payload == body

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
