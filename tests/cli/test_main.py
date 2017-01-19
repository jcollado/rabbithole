# -*- coding: utf-8 -*-

"""Main entry point test cases."""

from mock import patch

from rabbithole.cli import main


def test_exit_on_keyboard_interrupt():
    """Exit when user hits Ctrl+C."""
    with patch('rabbithole.cli.parse_arguments') as parse_arguments_, \
            patch('rabbithole.cli.configure_logging'), \
            patch('rabbithole.cli.create_block_instance'), \
            patch('rabbithole.cli.create_flow'), \
            patch('rabbithole.cli.run_input_blocks'), \
            patch('rabbithole.cli.time') as time:
        parse_arguments_().config = {
            'blocks': [
                {'name': '<block#1>'},
                {'name': '<block#2>'},
            ],
            'flows': ['<flow#1>', '<flow#2>'],
        }
        time.sleep.side_effect = KeyboardInterrupt
        return_code = main()
        assert return_code == 0
