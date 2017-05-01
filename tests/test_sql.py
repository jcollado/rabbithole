# -*- coding: utf-8 -*-

"""Database output block test cases."""

import pytest

from mock import (
    MagicMock as Mock,
    patch,
)
from sqlalchemy.exc import SQLAlchemyError

from rabbithole.sql import Database


@pytest.fixture(name='database')
def fixture_database():
    """Create database object."""
    return Database('sqlite://')


def test_is_connected(database):
    """Connection is open in instance."""
    assert not database.connection.closed


def test_partial_callback(database):
    """Callback returned with query parameter set when instance called."""
    query = '<query>'
    batch = [1, 2, 3]

    callback = database(query)
    database.connection = Mock()
    callback('<sender>', batch=batch)
    database.connection.execute.assert_called_once_with(query, batch)


def test_query_executed(database):
    """Query executed when batch is ready."""
    query = 'query'
    parameters = None
    batch = [1, 2, 3]

    database.connection = Mock()
    database.batch_ready_cb('<sender>', query, parameters, batch)
    database.connection.execute.assert_called_once_with(query, batch)


def test_batch_parameters(database):
    """Batch parameters mapped as expected."""
    query = 'query'
    parameters = ['message', 'nested.message']
    batch = [
        {
            'message': '<message>',
            'nested': {
                'message': '<nested_message>',
            },
        },
    ]

    database.connection = Mock()
    database.batch_ready_cb('<sender>', query, parameters, batch)
    database.connection.execute.assert_called_once_with(
        query,
        [['<message>', '<nested_message>']],
    )


def test_error_on_database_error(database):
    """Error written to logs on query execution failure."""
    query = 'query'
    batch = [1, 2, 3]
    parameters = []
    exception = SQLAlchemyError('<error>')

    database.connection = Mock()
    database.connection.execute.side_effect = exception
    with patch('rabbithole.sql.LOGGER') as logger:
        database.batch_ready_cb('<sender>', query, parameters, batch)
        assert logger.error.call_count == 2
