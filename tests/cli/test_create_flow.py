# -*- coding: utf-8 -*-

"""Create flow test cases."""

import pytest

from mock import (
    MagicMock as Mock,
    patch,
)

from rabbithole.cli import create_flow


@pytest.fixture(name='input_block')
def fixture_input_block():
    """Create mock input block."""
    return Mock()


@pytest.fixture(name='output_block')
def fixture_output_block():
    """Create mock output block."""
    return Mock()


@pytest.fixture(name='kwargs')
def fixture_kwargs(input_block, output_block):
    """Create flow kwargs."""
    return {
        'flow': [
            {'name': 'input'},
            {'name': 'output'},
        ],
        'namespace': {
            'input': input_block,
            'output': output_block,
        },
        'batcher_config': {},
    }


def test_signals_connected(input_block, output_block, kwargs):
    """Signal objects are connected as expected."""
    input_signal = input_block()
    output_cb = output_block()

    with patch('rabbithole.cli.Batcher') as batcher_cls:
        create_flow(**kwargs)

    input_signal.connect.assert_called_once_with(
        batcher_cls().message_received_cb,
        weak=False,
    )
    batcher_signal = batcher_cls().batch_ready
    batcher_signal.connect.assert_called_once_with(
        output_cb,
        weak=False,
    )


def test_exit_on_input_signal_error(input_block, kwargs):
    """Exit on error trying to get the input signal."""
    input_block.side_effect = Exception()
    with pytest.raises(SystemExit) as exc_info:
        create_flow(**kwargs)
    assert exc_info.value.code == 1


def test_exit_on_output_cb_error(output_block, kwargs):
    """Exit on error trying to get the output callback."""
    output_block.side_effect = Exception()
    with pytest.raises(SystemExit) as exc_info:
        create_flow(**kwargs)
    assert exc_info.value.code == 1
