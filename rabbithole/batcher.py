# -*- coding: utf-8 -*-

"""Batcher: group messages in batches before writing them to the database.

The strategy to batch messages is:
    - store them in memory as they are received
    - send them to the database when either the size or the time limit is
    exceeded.

"""

import logging
import threading

from collections import defaultdict

LOGGER = logging.getLogger(__name__)


class Batcher(object):

    """Group messages in batches before writing them to the database.

    A batch is written to the database when either the size or the time limit
    is exceeded.

    :param database: Database to use to insert message batches
    :type database: rabbithole.db.Database

    """

    SIZE_LIMIT = 5
    TIME_LIMIT = 15

    def __init__(self, database):
        """Initialize internal data structures."""
        self.database = database
        self.batches = defaultdict(list)
        self.locks = defaultdict(threading.Lock)
        self.timers = {}

    def message_received_cb(self, exchange_name, payload):
        """Handle message received event.

        This callback is executed when message is received by the AMQP
        consumer.

        :param exchange_name: Key used to determine which query to execute
        :type exchange_name: str
        :param payload: Rows to insert in the database
        :type payload: list(dict(str))

        """
        # Use a lock to make sure that callback execution doesn't interleave
        with self.locks[exchange_name]:
            batch = self.batches[exchange_name]
            batch.append(payload)
            LOGGER.debug(
                'Message added to %r batch (size: %d)',
                exchange_name,
                len(batch),
            )

            if len(batch) == 1:
                self.start_timer(exchange_name)
            elif len(batch) >= self.SIZE_LIMIT:
                LOGGER.debug(
                    'Size limit (%d) exceeded for %r',
                    self.SIZE_LIMIT,
                    exchange_name,
                )
                self.insert_batch(exchange_name)
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
                self.TIME_LIMIT,
                exchange_name,
            )
            self.insert_batch(exchange_name)
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

    def insert_batch(self, exchange_name):
        """Insert batch into database.

        A batch is inserted into the database either by the main thread when
        the size limit is exceeded or by a timer thread when the time limit is
        exceeded.

        :param exchange_name: Exchange from which message batch was received
        :type exchange_name: str

        """
        batch = self.batches[exchange_name]
        if not batch:
            LOGGER.warning('Nothing to insert: %r', exchange_name)
            return
        self.database.insert(exchange_name, self.batches[exchange_name])
        del self.batches[exchange_name]

    def start_timer(self, exchange_name):
        """Start timer thread.

        A timer thread is started to make sure that the batch will be inserted
        into the database if the time limit is exceeded before the size limit.

        :param exchange_name: Exchange from which message batch was received
        :type exchange_name: str

        """
        if exchange_name in self.timers:
            LOGGER.warning('Timer already active for: %r', exchange_name)
            return
        timer = threading.Timer(
            self.TIME_LIMIT,
            self.time_expired_cb,
            (exchange_name, ),
        )
        timer.name = 'timer-{}'.format(exchange_name)
        timer.daemon = True
        timer.start()
        LOGGER.debug('Timer thread started: (%d, %s)', timer.ident, timer.name)
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
