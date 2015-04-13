import sys

from mock import patch
import pytest

from nameko.cli.main import main, setup_parser
from nameko.exceptions import CommandError, ConfigurationError


@pytest.yield_fixture(autouse=True)
def fake_argv():
    with patch.object(
        sys,
        'argv',
        [
            'nameko',
            'run',
            '--broker',
            'my_broker',
            'test.sample:Service',
        ],
    ):
        yield


def test_run():
    with patch('nameko.cli.main.run.main') as run:
        main()
    assert run.call_count == 1
    (args,), _ = run.call_args
    assert args.broker == 'my_broker'


@pytest.mark.parametrize('exception', (CommandError, ConfigurationError))
def test_error(exception, capsys):
    with patch('nameko.cli.main.run.main') as run:
        run.side_effect = exception('boom')
        main()
    out, _ = capsys.readouterr()
    assert out.strip() == 'Error: boom'


@pytest.mark.parametrize(('param', 'value'), (
    (None, None),
    ('--rlwrap', True),
    ('--no-rlwrap', False),
))
def test_flag_action(param, value):
    parser = setup_parser()
    args = ['backdoor', 0]
    if param is not None:
        args.append(param)
    parsed = parser.parse_args(args)
    assert parsed.rlwrap is value
