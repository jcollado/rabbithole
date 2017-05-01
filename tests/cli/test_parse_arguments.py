# -*- coding: utf-8 -*-

"""Argument parsing test cases."""

import pytest
import yaml

from mock import patch
from six import StringIO
from six.moves import builtins

from rabbithole.cli import parse_arguments


def test_config_file_does_not_exist():
    """SystemExit is raised if the configuration file does not exist."""
    # Do not include error output in test output
    with pytest.raises(SystemExit), patch('rabbithole.cli.sys.stderr'):
        parse_arguments(['file-does-not-exist'])


def test_config_file_invalid():
    """SystemExit is raised if the configuration file is invalid."""
    with pytest.raises(SystemExit), \
            patch('rabbithole.cli.sys.stderr'), \
            patch('rabbithole.cli.os') as os_, \
            patch('{}.open'.format(builtins.__name__)) as open_:
        os_.path.isfile.return_value = True
        open_().__enter__.return_value = StringIO('>invalid yaml<')
        parse_arguments(['some file'])


def test_config_file_load_success():
    """Config file successfully loaded."""
    expected_value = {'a': 'value'}
    with patch('rabbithole.cli.os') as os_, \
            patch('{}.open'.format(builtins.__name__)) as open_:
        os_.path.isfile.return_value = True
        open_().__enter__.return_value = (
            StringIO(yaml.dump(expected_value)))
        args = parse_arguments(['some file'])

    assert args['config'] == expected_value
