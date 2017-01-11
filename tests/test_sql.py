# -*- coding: utf-8 -*-

"""Database output test cases."""

from unittest import TestCase

from mock import (
    MagicMock as Mock,
    patch,
)
from sqlalchemy.exc import SQLAlchemyError

from rabbithole.sql import Database


class TestDatabase(TestCase):

    """Database test cases."""

    def setUp(self):
        """Create database object."""
        self.database = Database('sqlite://')

    def test_is_connected(self):
        """Connection is open in instance."""
        self.assertFalse(self.database.connection.closed)

    def test_partial_callback(self):
        """Callback returned with query parameter set when instance called."""
        raw_query = '<raw_query>'
        text_query = '<text_query>'
        batch = [1, 2, 3]

        with patch('rabbithole.sql.text') as text:
            text.return_value = text_query
            callback = self.database(raw_query)

        self.database.connection = Mock()
        callback('<sender>', batch=batch)
        self.database.connection.execute.assert_called_once_with(
            text_query, batch)

    def test_query_executed(self):
        """Query executed when batch is ready."""
        query = 'query'
        batch = [1, 2, 3]

        self.database.connection = Mock()
        self.database.batch_ready_cb('<sender>', query, batch)
        self.database.connection.execute.assert_called_once_with(query, batch)

    def test_error_on_database_error(self):
        """Error written to logs on query execution failure."""
        query = 'query'
        batch = [1, 2, 3]
        exception = SQLAlchemyError('<error>')

        self.database.connection = Mock()
        self.database.connection.execute.side_effect = exception
        with patch('rabbithole.sql.LOGGER') as logger:
            self.database.batch_ready_cb('<sender>', query, batch)
            self.assertEqual(logger.error.call_count, 2)
