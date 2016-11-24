# -*- coding: utf-8 -*-

import logging

from sqlalchemy import (
    create_engine,
    text,
)
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class Database(object):
    """Database writer.

    :param url: Database connection string
    :type url: str
    :param queries: Insert query to execute for each exchange name
    :type queries: dict(str, str)

    """

    def __init__(self, url, queries):
        """Connect to database."""
        engine = create_engine(url)

        self.connection = engine.connect()
        logger.debug('Connected to: %r', url)

        self.queries = {
            exchange_name: text(query)
            for exchange_name, query
            in queries.items()
        }

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
            logger.error(exception)
        else:
            logger.debug('Inserted %d rows', len(rows))
