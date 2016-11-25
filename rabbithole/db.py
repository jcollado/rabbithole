# -*- coding: utf-8 -*-

"""Database: run queries with batches of rows per exchange."""

import logging

from sqlalchemy import (
    create_engine,
    text,
)
from sqlalchemy.exc import SQLAlchemyError

LOGGER = logging.getLogger(__name__)


class Database(object):
    """Database writer.

    :param url: Database connection string
    :type url: str
    :param queries: Insert query to execute for each exchange name
    :type queries: dict(str, str)

    """

    def __init__(self, url, queries):
        """Connect to database."""
        self.engine = create_engine(url)
        self.connection = None

        self.queries = {
            exchange_name: text(query)
            for exchange_name, query
            in queries.items()
        }

    def connect(self):
        """Connect to the database."""
        self.connection = self.engine.connect()
        LOGGER.debug('Connected to: %r', self.engine.url)
        return self

    def insert(self, exchange_name, rows):
        """Insert rows in database.

        :param exchange_name: Key used to determine which query to execute
        :type exchange_name: str
        :param rows: Row data to insert
        :type rows: list(dict(str))

        """
        assert exchange_name in self.queries
        query = self.queries[exchange_name]
        try:
            self.connection.execute(query, rows)
        except SQLAlchemyError as exception:
            LOGGER.error(exception)
        else:
            LOGGER.debug('Inserted %d rows', len(rows))
