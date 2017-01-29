# -*- coding: utf-8 -*-

"""Batcher: group messages in batches before sending them to the output.

The strategy to batch messages is:
    - store them in memory as they are received
    - send them to the output when either the size or the time limit is
      exceeded.

"""

import logging
import threading

import blinker

from typing import (  # noqa
    Dict,
    List,
    Optional,
)

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

    def __init__(self, size_limit=None, time_limit=None):
        # type: (int, int) -> None
        """Initialize internal data structures."""
        self.size_limit = size_limit or self.DEFAULT_SIZE_LIMIT
        self.time_limit = time_limit or self.DEFAULT_TIME_LIMIT

        self.batch = []  # type: List[Dict[str, object]]
        self.lock = threading.Lock()
        self.timer = None  # type: Optional[threading.Timer]
        self.batch_ready = blinker.Signal()

    def message_received_cb(self, sender, payload):
        # type: (object, Dict[str, object]) -> None
        """Handle message received event.

        This callback is executed when message is received by the AMQP
        consumer.

        :param sender: The consumer who sent the message
        :type sender: object
        :param payload: Record to send to the output
        :type payload: dict(str)

        """
        # Use a lock to make sure that callback execution doesn't interleave
        with self.lock:
            self.batch.append(payload)
            LOGGER.debug(
                'Message added to batch (size: %d, capacity: %d)',
                len(self.batch),
                self.size_limit,
            )

            if len(self.batch) == 1:
                self.start_timer()
            elif len(self.batch) >= self.size_limit:
                LOGGER.debug('Size limit (%d) exceeded', self.size_limit)
                self.queue_batch()
                self.cancel_timer()

    def time_expired_cb(self):
        # type: () -> None
        """Handle time expired event.

        This callback is executed in a timer thread when the time limit for a
        batch of messages has been exceeded.

        """
        # Use a lock to make sure that callback execution doesn't interleave
        with self.lock:
            LOGGER.debug('Time limit (%.2f) exceeded', self.time_limit)
            if self.timer is None:
                LOGGER.warning('Timer is not active')
                return
            self.queue_batch()
            self.timer = None

        thread = threading.current_thread()
        LOGGER.debug(
            'Timer thread finished: (%d, %s)',
            thread.ident,
            thread.name,
        )

    def queue_batch(self):
        # type: () -> None
        """Queue batch before sending to the output.

        A batch is queued either by the main thread when the size limit is
        exceeded or by a timer thread when the time limit is exceeded.

        :param exchange_name: Exchange from which message batch was received
        :type exchange_name: str

        """
        if not self.batch:
            LOGGER.warning('Nothing to queue')
            return
        self.batch_ready.send(self, batch=self.batch)
        self.batch = []

    def start_timer(self):
        # type: () -> None
        """Start timer thread.

        A timer thread is started to make sure that the batch will be sent to
        the output if the time limit is exceeded before the size limit.

        """
        if self.timer:
            LOGGER.warning('Timer already active')
            return
        timer = threading.Timer(self.time_limit, self.time_expired_cb)
        timer.daemon = True
        timer.start()
        LOGGER.debug(
            'Timer thread started (%.2f): (%d, %s)',
            self.time_limit,
            timer.ident,
            timer.name,
        )
        self.timer = timer

    def cancel_timer(self):
        # type: () -> None
        """Cancel timer thread.

        A timer thread might be cancelled if the size limit for a batch is
        exceeded before the time limit.

        :param exchange_name: Exchange from which message batch was received
        :type exchange_name: str

        """
        if self.timer is None:
            LOGGER.warning('Timer is not active')
            return
        self.timer.cancel()
        LOGGER.debug(
            'Timer thread cancelled: (%d, %s)',
            self.timer.ident,
            self.timer.name,
        )
        self.timer = None
