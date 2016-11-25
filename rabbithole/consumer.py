# -*- coding: utf-8 -*-

"""Consumer: get messages from rabbitmq.

The strategy to get messages is:
    - connect to the rabbitmq server
    - bind a queue to the desired exchanges

Note that it's assumed that the exchanges will have `fanout` type and that the
routing key isn't relevant in this case.

"""

import json
import logging

import blinker
import pika

LOGGER = logging.getLogger(__name__)


class Consumer(object):

    """Rabbitmq message consumer.

    :param server: Rabbitmq server IP address
    :type server: str
    :param exchange_names: Exchange names to bind to
    :type exchange_names: list(str)

    """

    def __init__(self, server, exchange_names):
        """Configure exchanges and queue."""
        LOGGER.info('Connecting to %r...', server)
        parameters = pika.ConnectionParameters(server)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        # Use a single queue to process messages from all exchanges
        result = channel.queue_declare(auto_delete=True)
        queue_name = result.method.queue
        LOGGER.debug('Declared queue %r', queue_name)

        for exchange_name in exchange_names:
            channel.exchange_declare(
                exchange=exchange_name,
                exchange_type='fanout',
            )
            channel.queue_bind(
                exchange=exchange_name,
                queue=queue_name,
            )
            LOGGER.debug(
                'Queue %r bound to exchange %r', queue_name, exchange_name)

        channel.basic_consume(self.message_received_cb, queue=queue_name)

        self.channel = channel
        self.message_received = blinker.Signal()

    def run(self):
        """Run ioloop and consume messages."""
        logging.info('Waiting for messages...')
        self.channel.start_consuming()

    def message_received_cb(self, channel, method_frame, header_frame, body):
        """Handle message received.

        :param channel: Connection channel with rabbitmq server
        :type channel: pika.channel.Channel
        :param method_frame: AMPQ method related data
        :type method_frame: pika.spec.Deliver
        :param body: Message body
        :type body: str

        """
        exchange_name = method_frame.exchange
        LOGGER.debug('Message received from %r: %s', exchange_name, body)

        # Only accept json messages
        if header_frame.content_type != 'application/json':
            LOGGER.warning(
                'Message discarded. Unexpected content type: %r',
                header_frame.content_type,
            )
            channel.basic_nack(method_frame.delivery_tag, requeue=False)
            return

        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        payload = json.loads(body)
        self.message_received.send(exchange_name, payload=payload)
