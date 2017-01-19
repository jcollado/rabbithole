# -*- coding: utf-8 -*-

"""Run input blocks test cases."""

from mock import MagicMock as Mock

from rabbithole.cli import run_input_blocks


def test_run_method_called():
    """Run method is called."""
    block_instance = Mock()
    namespace = {
        '<name>': block_instance,
    }
    threads = run_input_blocks(namespace)
    for thread in threads:
        thread.join()
    block_instance.run.assert_called_once_with()
