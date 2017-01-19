# -*- coding: utf-8 -*-

"""Database output block test cases."""

import pytest

from mock import (
    MagicMock as Mock,
    patch,
)
from sqlalchemy.exc import SQLAlchemyError

from rabbithole.sql import Database


@pytest.fixture
def database():
    """Create database object."""
    return Database('sqlite://')


def test_is_connected(database):
    """Connection is open in instance."""
    assert not database.connection.closed


def test_partial_callback(database):
    """Callback returned with query parameter set when instance called."""
    raw_query = '<raw_query>'
    text_query = '<text_query>'
    batch = [1, 2, 3]

    with patch('rabbithole.sql.text') as text:
        text.return_value = text_query
        callback = database(raw_query)

    database.connection = Mock()
    callback('<sender>', batch=batch)
    database.connection.execute.assert_called_once_with(text_query, batch)


def test_query_executed(database):
    """Query executed when batch is ready."""
    query = 'query'
    batch = [1, 2, 3]

    database.connection = Mock()
    database.batch_ready_cb('<sender>', query, batch)
    database.connection.execute.assert_called_once_with(query, batch)


def test_error_on_database_error(database):
    """Error written to logs on query execution failure."""
    query = 'query'
    batch = [1, 2, 3]
    exception = SQLAlchemyError('<error>')

    database.connection = Mock()
    database.connection.execute.side_effect = exception
    with patch('rabbithole.sql.LOGGER') as logger:
        database.batch_ready_cb('<sender>', query, batch)
        assert logger.error.call_count == 2
