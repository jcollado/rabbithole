# -*- coding: utf-8 -*-

"""Create block instance test cases."""

import pytest

from mock import (
    MagicMock as Mock,
    patch,
)

from rabbithole.cli import create_block_instance

BLOCK_NAME = '<block_name>'
BLOCK_TYPE = '<block_type>'


@pytest.fixture(name='block_class')
def fixture_block_class():
    """Patch available block classes."""
    block_class = Mock()

    patcher = patch.dict(
        'rabbithole.cli.BLOCK_CLASSES',
        {BLOCK_TYPE: block_class},
    )
    patcher.start()
    yield block_class
    patcher.stop()


def test_block_instance_created(block_class):
    """Create block instance successfully."""
    create_block_instance({
        'name': BLOCK_NAME,
        'type': BLOCK_TYPE,
        'args': [1, 2, 3],
        'kwargs': {'a': 1, 'b': 2, 'c': 3}
    })
    block_class.assert_called_once_with(1, 2, 3, a=1, b=2, c=3)


def test_exit_on_instance_error(block_class):
    """Exit on block instantiation error."""
    block_class.side_effect = Exception
    with pytest.raises(SystemExit) as exc_info:
        create_block_instance({
            'name': BLOCK_NAME,
            'type': BLOCK_TYPE,
        })
    assert exc_info.value.code == 1
