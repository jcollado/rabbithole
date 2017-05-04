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
    parameters = None
    batch = [1, 2, 3]

    database.connection = Mock()
    database.batch_ready_cb('<sender>', query, parameters, batch)
    database.connection.execute.assert_called_once_with(query, batch)


def test_list_parameters(database):
    """List parameters mapped as expected."""
    query = 'query'
    parameters = [
        'message',
        'count',
        'nested',
        'nested.message',
        'unknown',
        'nested.unknown',
        'message.unknown',
        'count.unknown',
    ]
    batch = [
        {
            'message': '<message>',
            'count': 42,
            'nested': {
                'message': '<nested_message>',
            },
        },
    ]

    database.connection = Mock()
    database.batch_ready_cb('<sender>', query, parameters, batch)
    database.connection.execute.assert_called_once_with(
        query,
        [
            [
                '<message>',
                42,
                '{"message": "<nested_message>"}',
                '<nested_message>',
                None,
                None,
                None,
                None,
            ],
        ],
    )


def test_dict_parameters(database):
    """Dict parameters mapped as expected."""
    query = 'query'
    parameters = {
        'message': 'message',
        'count': 'count',
        'nested': 'nested',
        'nested_message': 'nested.message',
        'unknown': 'unknown',
        'nested_unknown': 'nested.unknown',
        'message_unknown': 'message.unknown',
        'count_unknown': 'count.unknown',
    }
    batch = [
        {
            'message': '<message>',
            'count': 42,
            'nested': {
                'message': '<nested_message>',
            },
        },
    ]

    database.connection = Mock()
    database.batch_ready_cb('<sender>', query, parameters, batch)
    database.connection.execute.assert_called_once_with(
        query,
        [
            {
                'message': '<message>',
                'count': 42,
                'nested': '{"message": "<nested_message>"}',
                'nested_message': '<nested_message>',
                'unknown': None,
                'nested_unknown': None,
                'message_unknown': None,
                'count_unknown': None,
            },
        ],
    )


def test_invalid_parameters(database):
    """Exception raised when invalid parameters passed."""
    query = 'query'
    parameters = 'invalid'
    batch = []

    database.connection = Mock()

    with pytest.raises(ValueError):
        database.batch_ready_cb('<sender>', query, parameters, batch)


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
