# -*- coding: utf-8 -*-

"""Database: run queries with batches of rows per exchange."""

import logging
import traceback

from functools import partial

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

    """

    def __init__(self, url):
        # type: (str) -> None
        """Create database engine."""
        engine = create_engine(url)
        self.connection = engine.connect()
        LOGGER.debug('Connected to: %r', url)

    def __call__(self, query):
        # type: (str) -> partial
        """Return callback to use when a batch is ready.

        :param query: The query to execute to insert the batch
        :type query: str

        """
        return partial(self.batch_ready_cb, query=text(query))

    def batch_ready_cb(self, sender, query, batch):
        # type: (object, object, List[Dict[str, object]]) -> None
        """Execute insert query for the batch that is ready.

        :param sender: The batcher who sent the batch_ready signal
        :type sender: rabbithole.batcher.Batcher
        :param query: The query to execute to insert the batch
        :type query: :class:`sqlalchemy.sql.elements.TextClause`
        :param batch: Batch of rows to insert
        :type rows: list(dict(str))

        """
        try:
            self.connection.execute(query, batch)
        except SQLAlchemyError:
            LOGGER.error(traceback.format_exc())
            LOGGER.error(
                'Query execution error:\n- query: %s\n- batch: %r',
                query,
                batch,
            )
        else:
            LOGGER.debug('Inserted %d rows', len(batch))
