# -*- coding: utf-8 -*-

"""Database: run queries with batches of rows per exchange."""

import json
import logging
import traceback

from functools import partial

import six

from sqlalchemy import (
    create_engine,
    text,
)
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
            query=text(query),
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
        elif isinstance(parameters, list):
            batch_parameters = ListParametersMapper(parameters).map(batch)
        elif isinstance(parameters, dict):
            batch_parameters = DictParametersMapper(parameters).map(batch)
        else:
            raise ValueError('Unexpected parameter mapping: %s', parameters)

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


class ListParametersMapper(object):

    """Map messages to lists of parameters.

    :param parameters: Mapping from message to query parameters.
    :type parameters: list(str)

    """

    def __init__(self, parameters):
        # type: (List[str]) -> None
        """Initialize parameters."""
        self.parameters = parameters

    def map(self, batch):
        # type: (List[Dict[str, object]]) -> List[List[Optional[object]]]
        """Get query parameters for a batch of messages.

        :param batch: Batch of messages
        :type batch: list(dict(str))
        :returns: All parameters extracted from all messages
        :rtype: list(list(object | None))

        """
        batch_parameters = [
            self._map_message_parameters(message)
            for message in batch
        ]
        return batch_parameters

    def _map_message_parameters(self, message):
        # type: (Dict[str, object]) -> List[Optional[object]]
        """Get query parameters for a message.

        :param message: A message
        :type message: dict(str)
        :returns: All parameters extracted from message
        :rtype: list(object | None)

        """
        message_parameters = [
            self._map_message_parameter(parameter, message)
            for parameter in self.parameters
        ]
        return message_parameters

    def _map_message_parameter(self, parameter, message):
        # type: (str, Dict[str, object]) -> Optional[object]
        """Get query parameter for a message.

        :param parameters: Mapping from message to query parameter.
        :type parameters: str
        :param message: A message
        :type message: dict(str)
        :returns: The parameter extracted from the message
        :rtype: object | None

        """
        keys = parameter.split('.')
        value = message
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        if isinstance(value, (list, dict)):
            value = json.dumps(value)
        return value


class DictParametersMapper(object):

    """Map messages to lists of parameters."""

    def __init__(self, parameters):
        """Initialize paremeters."""
        self.parameters = parameters

    def map(self, batch):
        # type: (List[Dict[str, object]]) -> List[Dict[str, Optional[object]]]
        """Get query parameters for a batch of messages.

        :param parameters: Mapping from message to query parameters.
        :type parameters: list(str)
        :param batch: Batch of messages
        :type batch: list(dict(str))
        :returns: All parameters extracted from all messages
        :rtype: list(list(object | None))

        """
        batch_parameters = [
            self._map_message_parameters(message)
            for message in batch
        ]
        return batch_parameters

    def _map_message_parameters(self, message):
        # type: (Dict[str, object]) -> Dict[str, Optional[object]]
        """Get query parameters for a message.

        :param parameters: Mapping from message to query parameters.
        :type parameters: list(str)
        :param message: A message
        :type message: dict(str)
        :returns: All parameters extracted from message
        :rtype: list(object | None)

        """
        message_parameters = {
            key: self._map_message_parameter(parameter, message)
            for key, parameter in six.iteritems(self.parameters)
        }
        return message_parameters

    def _map_message_parameter(self, parameter, message):
        # type: (str, Dict[str, object]) -> Optional[object]
        """Get query parameter for a message.

        :param parameters: Mapping from message to query parameter.
        :type parameters: str
        :param message: A message
        :type message: dict(str)
        :returns: The parameter extracted from the message
        :rtype: object | None

        """
        keys = parameter.split('.')
        value = message
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        if isinstance(value, (list, dict)):
            value = json.dumps(value)
        return value
