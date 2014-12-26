from contextlib import contextmanager
import sys

from mock import patch
import pytest

from nameko.cli.main import main
from nameko.cli.exceptions import CommandError


@contextmanager
def fake_argv(argv):
    with patch.object(sys, 'argv', ['nameko'] + argv
    ):
        yield


def test_run():
    with fake_argv([ 'run', '--broker', 'my_broker', 'test.sample:Service']):
        with patch('nameko.cli.main.run.main') as run:
            main()
    assert run.call_count == 1
    (args,), _ = run.call_args
    assert args.broker == 'my_broker'


def test_error(capsys):
    with fake_argv([ 'run', '--broker', 'my_broker', 'test.sample:Service']):
        with patch('nameko.cli.main.run.main') as run:
            run.side_effect = CommandError('boom')
            main()
    out, _ = capsys.readouterr()
    assert out.strip() == 'Error: boom'
