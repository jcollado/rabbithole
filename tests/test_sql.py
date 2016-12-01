# -*- coding: utf-8 -*-

"""Database output test cases."""

from unittest import TestCase

from mock import (
    MagicMock as Mock,
    patch,
)
from sqlalchemy.exc import SQLAlchemyError

from rabbithole.sql import Database


class TestConnect(TestCase):

    """Database connection test cases."""

    def test_is_connected(self):
        """Connection is open after call to connect."""
        database = Database('sqlite://', {}).connect()
        self.assertFalse(database.connection.closed)


class TestBatchReady(TestCase):

    """Database batch ready test cases."""

    def setUp(self):
        """Create database object."""
        self.database = Database(
            'sqlite://',
            {
                'exchange#1': 'query#1',
                'exchange#2': 'query#2',
            },
        ).connect()

    def test_warning_on_query_not_found(self):
        """Warning written to logs if no query found."""
        exchange_name = '<exchange>'

        with patch('rabbithole.sql.LOGGER') as logger:
            self.database.batch_ready_cb('<sender>', exchange_name, [])
            logger.warning.assert_called_once_with(
                'No query found for %r',
                exchange_name,
            )

    def test_query_executed(self):
        """Query executed for the given exchange."""
        exchange_name = 'exchange#1'
        batch = [1, 2, 3]

        self.database.connection = Mock()
        self.database.batch_ready_cb('<sender>', exchange_name, batch)
        self.database.connection.execute.assert_called_once_with(
            self.database.queries[exchange_name],
            batch,
        )

    def test_eror_on_database_error(self):
        """Error written to logs on query execution failure."""
        exchange_name = 'exchange#1'
        batch = [1, 2, 3]
        exception = SQLAlchemyError('<error>')

        self.database.connection = Mock()
        self.database.connection.execute.side_effect = exception
        with patch('rabbithole.sql.LOGGER') as logger:
            self.database.batch_ready_cb('<sender>', exchange_name, batch)
            logger.error.assert_called_once_with(exception)
