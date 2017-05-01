# -*- coding: utf-8 -*-

"""Database: run queries with batches of rows per exchange."""

import logging
import traceback

from functools import partial

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from typing import (  # noqa
    List,
    Optional,
)

LOGGER = logging.getLogger(__name__)


class Database(object):

    """Database writer.

    :param url: Database connection string
    :type url: str

    """

    def __init__(self, url):
        # type: (str) -> None
        """Create database engine."""
        engine = create_engine(url)
        self.connection = engine.connect()
        LOGGER.debug('Connected to: %r', url)

    def __call__(self, query, parameters=None):
        # type: (str, Optional[List]) -> partial
        """Return callback to use when a batch is ready.

        :param query: The query to execute to insert the batch
        :type query: str
        :param parameters: Parameters to pass to the query on execution
        :type parameters: list | None

        """
        return partial(
            self.batch_ready_cb,
            query=query,
            parameters=parameters,
        )

    def batch_ready_cb(
            self,
            sender,  # type: object
            query,  # type: object
            parameters,  # type: Optional[List]
            batch,  # type: List[Dict[str, object]]
                ):
        """Execute insert query for the batch that is ready.

        :param sender: The batcher who sent the batch_ready signal
        :type sender: rabbithole.batcher.Batcher
        :param query: The query to execute to insert the batch
        :type query: :class:`sqlalchemy.sql.elements.TextClause`
        :param batch: Batch of messages
        :type batch: list(dict(str))

        """
        if parameters is None:
            batch_parameters = batch
        else:
            batch_parameters = self._get_batch_parameters(parameters, batch)

        try:
            LOGGER.info(
                'Executing query: (query: %s, parameters: %s)',
                query,
                batch_parameters,
            )
            self.connection.execute(query, batch_parameters)
        except SQLAlchemyError:
            LOGGER.error(traceback.format_exc())
            LOGGER.error(
                'Query execution error:\n- query: %s\n- batch: %r',
                query,
                batch,
            )
        else:
            LOGGER.debug('Inserted %d rows', len(batch))

    def _get_batch_parameters(self, parameters, batch):
        """Get query parameters for a batch of messages.

        :param parameters: Mapping from message to query parameters.
        :type parameters: list(str)
        :param batch: Batch of messages
        :type batch: list(dict(str))

        """
        batch_parameters = [
            self._get_message_parameters(parameters, message)
            for message in batch
        ]
        return batch_parameters

    def _get_message_parameters(self, parameters, message):
        """Get query parameters for a message.

        :param parameters: Mapping from message to query parameters.
        :type parameters: list(str)
        :param message: A message
        :type message: dict(str)

        """
        message_parameters = [
            self._get_message_parameter(parameter, message)
            for parameter in parameters
        ]
        return message_parameters

    def _get_message_parameter(self, parameter, message):
        """Get query parameter for a message.

        :param parameters: Mapping from message to query parameter.
        :type parameters: str
        :param message: A message
        :type message: dict(str)

        """
        keys = parameter.split('.')
        value = message
        for key in keys:
            value = value.get(key)
            if value is None:
                break
        return value
