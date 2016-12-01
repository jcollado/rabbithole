# -*- coding: utf-8 -*-

"""Batcher: group messages in batches before sending them to the output

The strategy to batch messages is:
    - store them in memory as they are received
    - send them to the output when either the size or the time limit is
    exceeded.

"""

import logging
import threading

from collections import defaultdict

import blinker

LOGGER = logging.getLogger(__name__)


class Batcher(object):

    """Group messages in batches before sending them to the output.

    A batch is sent to the output when either the size or the time limit is
    exceeded.

    :param size_limit: Capacity of the batcher in number of messages
    :type size_limit: int
    :param time_limit: Time before sending batch to the output in seconds
    :type time_limit: int

    """

    DEFAULT_SIZE_LIMIT = 5
    DEFAULT_TIME_LIMIT = 15

    def __init__(self, size_limit, time_limit):
        """Initialize internal data structures."""
        self.size_limit = size_limit or self.DEFAULT_SIZE_LIMIT
        self.time_limit = time_limit or self.DEFAULT_TIME_LIMIT

        self.batches = defaultdict(list)
        self.locks = defaultdict(threading.Lock)
        self.timers = {}
        self.batch_ready = blinker.Signal()

    def message_received_cb(self, sender, exchange_name, payload):
        """Handle message received event.

        This callback is executed when message is received by the AMQP
        consumer.

        :param sender: The consumer who sent the message
        :type sender: rabbithole.consumer.Consumer
        :param exchange_name: Key used to determine which query to execute
        :type exchange_name: str
        :param payload: Records to send to the output
        :type payload: list(dict(str))

        """
        # Use a lock to make sure that callback execution doesn't interleave
        with self.locks[exchange_name]:
            batch = self.batches[exchange_name]
            batch.append(payload)
            LOGGER.debug(
                'Message added to %r batch (size: %d, capacity: %d)',
                exchange_name,
                len(batch),
                self.size_limit,
            )

            if len(batch) == 1:
                self.start_timer(exchange_name)
            elif len(batch) >= self.size_limit:
                LOGGER.debug(
                    'Size limit (%d) exceeded for %r',
                    self.size_limit,
                    exchange_name,
                )
                self.queue_batch(exchange_name)
                self.cancel_timer(exchange_name)

    def time_expired_cb(self, exchange_name):
        """Handle time expired event.

        This callback is executed in a timer thread when the time limit for a
        batch of messages has been exceeded.

        :param exchange_name: Exchange from which message batch was received
        :type exchange_name: str

        """
        # Use a lock to make sure that callback execution doesn't interleave
        with self.locks[exchange_name]:
            LOGGER.debug(
                'Time limit (%.2f) exceeded for %r',
                self.time_limit,
                exchange_name,
            )
            self.queue_batch(exchange_name)
            if exchange_name not in self.timers:
                LOGGER.warning('Timer not found for: %r', exchange_name)
                return
            del self.timers[exchange_name]

        thread = threading.current_thread()
        LOGGER.debug(
            'Timer thread finished: (%d, %s)',
            thread.ident,
            thread.name,
        )

    def queue_batch(self, exchange_name):
        """Queue batch before sending to the output.

        A batch is queued either by the main thread when the size limit is
        exceeded or by a timer thread when the time limit is exceeded.

        :param exchange_name: Exchange from which message batch was received
        :type exchange_name: str

        """
        batch = self.batches[exchange_name]
        if not batch:
            LOGGER.warning('Nothing to queue for %r', exchange_name)
            return
        self.batch_ready.send(
            self,
            exchange_name=exchange_name,
            batch=batch,
        )
        del self.batches[exchange_name]

    def start_timer(self, exchange_name):
        """Start timer thread.

        A timer thread is started to make sure that the batch will be sent to
        the output if the time limit is exceeded before the size limit.

        :param exchange_name: Exchange from which message batch was received
        :type exchange_name: str

        """
        if exchange_name in self.timers:
            LOGGER.warning('Timer already active for: %r', exchange_name)
            return
        timer = threading.Timer(
            self.time_limit,
            self.time_expired_cb,
            (exchange_name, ),
        )
        timer.name = 'timer-{}'.format(exchange_name)
        timer.daemon = True
        timer.start()
        LOGGER.debug(
            'Timer thread started (%.2f) for %r: (%d, %s)',
            self.time_limit,
            exchange_name,
            timer.ident,
            timer.name,
        )
        self.timers[exchange_name] = timer

    def cancel_timer(self, exchange_name):
        """Cancel timer thread.

        A timer thread might be cancelled if the size limit for a batch is
        exceeded before the time limit.

        :param exchange_name: Exchange from which message batch was received
        :type exchange_name: str

        """
        timer = self.timers.get(exchange_name)
        if timer is None:
            LOGGER.warning('Timer not found for: %r', exchange_name)
            return
        timer.cancel()
        LOGGER.debug(
            'Timer thread cancelled: (%d, %s)',
            timer.ident,
            timer.name,
        )
        del self.timers[exchange_name]
