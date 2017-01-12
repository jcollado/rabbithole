# -*- coding: utf-8 -*-

"""CLI test cases."""

import logging

from unittest import TestCase

import yaml

from mock import (
    Mock,
    patch,
)
from six import StringIO

from rabbithole.cli import (
    configure_logging,
    create_block_instance,
    create_flow,
    main,
    parse_arguments,
    run_input_blocks,
)


class TestMain(TestCase):

    """Main entry point test cases."""

    def test_exit_on_keyboard_interrupt(self):
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
            self.assertEqual(return_code, 0)


class TestCreateBlockInstance(TestCase):

    """Create block instance test cases."""
    BLOCK_NAME = '<block_name>'
    BLOCK_TYPE = '<block_type>'

    def setUp(self):
        """Patch available block classes."""
        self.block_class = Mock()

        patcher = patch.dict(
            'rabbithole.cli.BLOCK_CLASSES',
            {self.BLOCK_TYPE: self.block_class},
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_block_instance_created(self):
        """Create block instance successfully."""
        create_block_instance({
            'name': self.BLOCK_NAME,
            'type': self.BLOCK_TYPE,
            'args': [1, 2, 3],
            'kwargs': {'a': 1, 'b': 2, 'c': 3}
        })
        self.block_class.assert_called_once_with(1, 2, 3, a=1, b=2, c=3)

    def test_exit_on_block_instantiation_error(self):
        """Exit on block instantiation error."""
        self.block_class.side_effect = Exception
        with self.assertRaises(SystemExit) as context:
            create_block_instance({
                'name': self.BLOCK_NAME,
                'type': self.BLOCK_TYPE,
            })
        self.assertEqual(context.exception.code, 1)


class TestCreateFlow(TestCase):

    """Create flow test cases."""

    def setUp(self):
        """Create block instances mocks."""
        self.input_block = Mock()
        self.output_block = Mock()
        self.kwargs = {
            'flow': [
                {'name': 'input'},
                {'name': 'output'},
            ],
            'namespace': {
                'input': self.input_block,
                'output': self.output_block,
            },
            'batcher_config': {},
        }

    def test_signals_connected(self):
        """Signals are connected as expected."""
        input_signal = self.input_block()
        output_cb = self.output_block()

        with patch('rabbithole.cli.Batcher') as batcher_cls:
            create_flow(**self.kwargs)

        input_signal.connect.assert_called_once_with(
            batcher_cls().message_received_cb,
            weak=False,
        )
        batcher_signal = batcher_cls().batch_ready
        batcher_signal.connect.assert_called_once_with(
            output_cb,
            weak=False,
        )

    def test_exit_on_input_signal_error(self):
        """Exit on error trying to get the input signal."""
        self.input_block.side_effect = Exception()
        with self.assertRaises(SystemExit) as context:
            create_flow(**self.kwargs)
        self.assertEqual(context.exception.code, 1)

    def test_exit_on_output_cb_error(self):
        """Exit on error trying to get the output callback."""
        self.output_block.side_effect = Exception()
        with self.assertRaises(SystemExit) as context:
            create_flow(**self.kwargs)
        self.assertEqual(context.exception.code, 1)


class TestRunInputBlocks(TestCase):

    """Run input blocks test cases."""

    def test_run_method_called(self):
        """Run method is called."""
        block_instance = Mock()
        namespace = {
            '<name>': block_instance,
        }
        run_input_blocks(namespace)
        block_instance.run.assert_called_once_with()


class TestParseArguments(TestCase):

    """Argument parsing test cases."""

    def test_config_file_does_not_exist(self):
        """SystemExit is raised if the configuration file does not exist."""
        # Do not include error output in test output
        with self.assertRaises(SystemExit), patch('rabbithole.cli.sys.stderr'):
            parse_arguments(['file-does-not-exist'])

    def test_config_file_invalid(self):
        """SystemExit is raised if the configuration file is invalid."""
        with self.assertRaises(SystemExit), \
                patch('rabbithole.cli.sys.stderr'), \
                patch('rabbithole.cli.os') as os_, \
                patch('rabbithole.cli.open') as open_:
            os_.path.isfile.return_value = True
            open_().__enter__.return_value = StringIO('>invalid yaml<')
            parse_arguments(['some file'])

    def test_config_file_load_success(self):
        """Config file successfully loaded."""
        expected_value = {'a': 'value'}
        with patch('rabbithole.cli.os') as os_, \
                patch('rabbithole.cli.open') as open_:
            os_.path.isfile.return_value = True
            open_().__enter__.return_value = (
                StringIO(yaml.dump(expected_value)))
            args = parse_arguments(['some file'])

        self.assertDictEqual(args.config, expected_value)


class TestConfigureLogging(TestCase):

    """Logging configuration test cases."""

    def tearDown(self):
        """Delete root logger handlers."""
        root_logger = logging.getLogger()
        root_logger.handlers = []

    def test_root_level_set_to_debug(self):
        """Root logger level set to debug."""
        configure_logging(logging.ERROR)
        root_logger = logging.getLogger()
        self.assertEqual(root_logger.level, logging.DEBUG)

    def test_stream_handler_level(self):
        """Stream handler level set to argument value."""
        expected_value = logging.ERROR
        configure_logging(expected_value)
        root_logger = logging.getLogger()
        self.assertEqual(len(root_logger.handlers), 1)
        handler = root_logger.handlers[0]
        self.assertEqual(handler.level, expected_value)

    def test_pika_level_set_warning(self):
        """Pika logger level is set to warning."""
        configure_logging(logging.DEBUG)
        pika_logger = logging.getLogger('pika')
        self.assertEqual(pika_logger.level, logging.WARNING)
