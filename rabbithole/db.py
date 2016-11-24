# -*- coding: utf-8 -*-

import logging

from sqlalchemy import (
    create_engine,
    text,
)

logger = logging.getLogger(__name__)


class Database(object):
    """Database writer.

    :param url: Database connection string
    :type url: str

    """

    def __init__(self, url, insert_query):
        """Connect to database."""
        engine = create_engine(url)

        self.connection = engine.connect()
        logger.debug('Connected to: %r', url)

        self.insert_query = text(insert_query)

    def insert(self, rows):
        """Insert rows in database."""
        self.connection.execute(self.insert_query, rows)
        logger.debug('Inserted %d rows', len(rows))
