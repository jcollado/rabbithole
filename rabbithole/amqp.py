# -*- coding: utf-8 -*-

"""Consumer: get messages from amqp server.

The strategy to get messages is:
    - connect to the amqp server
    - bind a queue to the desired exchanges

Note that it's assumed that the exchanges will have `fanout` type and that the
routing key isn't relevant in this case.

"""

import json
import logging

import blinker
import pika

from typing import Dict  # noqa

LOGGER = logging.getLogger(__name__)


class Consumer(object):

    """AMQP message consumer.

    :param url: AMQP server connection string
    :type server: str

    """

    def __init__(self, url):
        # type: (str) -> None
        """Configure queue."""
        LOGGER.info('Connecting to %r...', url)
        parameters = pika.URLParameters(url)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        # Use a single queue to process messages from all exchanges
        result = channel.queue_declare(auto_delete=True)
        queue_name = result.method.queue
        LOGGER.debug('Declared queue %r', queue_name)

        channel.basic_consume(self.message_received_cb, queue=queue_name)

        self.channel = channel
        self.queue_name = queue_name
        self.signals = {}  # type: Dict[str, blinker.Signal]

    def __call__(self, exchange_name):
        # type: (str) -> blinker.Signal
        """Create signal to send when a message from a exchange is received.

        :param exchange_name: Exchange name to bind to the queue
        :type exchange_name: str
        :returns: The signal that will be send, so that it can be connected
        :rtype: :class:`blinker.Signal`

        """
        if exchange_name in self.signals:
            return self.signals[exchange_name]

        self.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type='fanout',
        )
        self.channel.queue_bind(
            exchange=exchange_name,
            queue=self.queue_name,
        )
        LOGGER.debug(
            'Queue %r bound to exchange %r', self.queue_name, exchange_name)

        signal = blinker.Signal()
        self.signals[exchange_name] = signal
        return signal

    def run(self):
        # type: () -> None
        """Run ioloop and consume messages."""
        logging.info('Waiting for messages...')
        self.channel.start_consuming()

    def message_received_cb(self, channel, method_frame, header_frame, body):
        """Handle message received.

        :param channel: Connection channel with AMQP server
        :type channel: pika.channel.Channel
        :param method_frame: AMQP method related data
        :type method_frame: pika.spec.Deliver
        :param header_frame: AMQP message related data
        :type header_frame: pika.spec.BasicProperties
        :param body: Message body
        :type body: str

        """
        exchange_name = method_frame.exchange
        LOGGER.debug('Message received from %r: %s', exchange_name, body)

        # Only accept json messages
        if header_frame.content_type != 'application/json':
            LOGGER.warning(
                'Unexpected content type: %r', header_frame.content_type)

        try:
            payload = json.loads(body)
        except ValueError:
            LOGGER.warning('Body decoding error: %r', body)
            channel.basic_nack(method_frame.delivery_tag, requeue=False)
        else:
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            signal = self.signals[exchange_name]
            signal.send(self, payload=payload)
