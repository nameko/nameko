import os
import sys

import pytest
import yaml
from mock import patch

from nameko.cli.main import main, setup_parser, setup_yaml_parser
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
    with patch('nameko.cli.run.main') as run:
        main()
    assert run.call_count == 1
    (args,), _ = run.call_args
    assert args.broker == 'my_broker'


@pytest.mark.parametrize('exception', (CommandError, ConfigurationError))
def test_error(exception, capsys):
    with patch('nameko.cli.run.main') as run:
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


class TestConfigEnvironmentVariables(object):

    @pytest.mark.parametrize(('yaml_config', 'env_vars', 'expected_config'), [
        # no default value, no env value
        ('FOO: ${BAR}', {}, {'FOO': None}),
        # use default value if env value not provided
        ('FOO: ${BAR:foo}', {}, {'FOO': 'foo'}),
        # use env value
        ('FOO: ${BAR}', {'BAR': 'bar'}, {'FOO': 'bar'}),
        # use env value instead of default
        ('FOO: ${BAR:foo}', {'BAR': 'bar'}, {'FOO': 'bar'}),
        # handles multi line
        (
            """
            FOO: ${BAR:foo}
            BAR: ${FOO:bar}
            """,
            {'BAR': 'bar', 'FOO': 'foo'},
            {'FOO': 'bar', 'BAR': 'foo'}
        ),
        # quoted values don't work without explicit resolver
        ('FOO: "${BAR:foo}"', {'BAR': 'bar'}, {'FOO': '${BAR:foo}'}),
        # quoted values work only with explicit resolver
        ('FOO: !env_var "${BAR:foo}"', {'BAR': 'bar'}, {'FOO': 'bar'}),
        # $ sign can be used
        ('FOO: $bar', {}, {'FOO': '$bar'}),
        # multiple substitutions
        (
            'FOO: http://${BAR:foo}/${FOO:bar}',
            {'BAR': 'bar', 'FOO': 'foo'},
            {'FOO': 'http://bar/foo'}
        ),
        # can handle int, float and boolean
        (
            """
            FOO:
                - BAR: ${INT:1}
                - BAR: ${FLOAT:1.1}
                - BAR: ${BOOL:True}
            """,
            {}, {'FOO': [{'BAR': 1}, {'BAR': 1.1}, {'BAR': True}]}
        ),
        (
            """
            FOO:
                - BAR: ${INT}
                - BAR: ${FLOAT}
                - BAR: ${BOOL}
            """,
            {'INT': '1', 'FLOAT': '1.1', 'BOOL': 'True'},
            {'FOO': [{'BAR': 1}, {'BAR': 1.1}, {'BAR': True}]}
        ),
        # list of scalar values
        (
            """
            FOO:
                - ${INT_1}
                - ${INT_2}
                - ${INT_3}
            BAR:
                - 1
                - 2
                - 3
            """,
            {'INT_1': '1', 'INT_2': '2', 'INT_3': '3'},
            {'FOO': [1, 2, 3], 'BAR': [1, 2, 3]}
        ),
        # inline list
        (
            """
            FOO: ${LIST}
            BAR:
                - 1
                - 2
                - 3
            """,
            {"LIST": "[1,2,3]"},
            {'FOO': [1, 2, 3], 'BAR': [1, 2, 3]}
        ),
        # inline list with block style
        (
            """
            FOO: ${LIST}
            BAR:
                - 1
                - 2
                - 3
            """,
            {"LIST": "- 1\n- 2\n- 3"},
            {'FOO': [1, 2, 3], 'BAR': [1, 2, 3]}
        ),
        # inline list via explicit tag
        (
            """
            FOO: !env_var "${LIST}"
            BAR:
                - 1
                - 2
                - 3
            """,
            {"LIST": "[1,2,3]"},
            {'FOO': [1, 2, 3], 'BAR': [1, 2, 3]}
        ),
        # inline list with default
        (
            """
            FOO: ${LIST:[1,2,3]}
            BAR:
                - 1
                - 2
                - 3
            """,
            {},
            {'FOO': [1, 2, 3], 'BAR': [1, 2, 3]}
        ),
        # inline list containing list
        (
            """
            FOO: ${LIST}
            BAR:
                - 1
                - 2
                - 3
            """,
            {"LIST": "[1, 2, 3, ['a', 'b', 'c']]"},
            {'FOO': [1, 2, 3, ['a', 'b', 'c']], 'BAR': [1, 2, 3]}
        ),
        # inline dict
        (
            """
            FOO: ${DICT}
            BAR:
                - 1
                - 2
                - 3
            """,
            {"DICT": "{'one': 1, 'two': 2}"},
            {'FOO': {'one': 1, 'two': 2}, 'BAR': [1, 2, 3]}
        ),
        # inline dict with block style
        (
            """
            FOO: ${DICT}
            BAR:
                - 1
                - 2
                - 3
            """,
            {"DICT": "one: 1\ntwo: 2"},
            {'FOO': {'one': 1, 'two': 2}, 'BAR': [1, 2, 3]}
        )
    ])
    def test_environment_vars_in_config(
        self, yaml_config, env_vars, expected_config
    ):
        setup_yaml_parser()

        with patch.dict('os.environ'):
            for key, val in env_vars.items():
                os.environ[key] = val

            results = yaml.load(yaml_config)
            assert results == expected_config

    def test_cannot_recurse(self):

        setup_yaml_parser()

        yaml_config = """
            FOO: ${VAR1}
            BAR:
                - 1
                - 2
                - 3
        """

        with patch.dict('os.environ'):
            os.environ["VAR1"] = "${VAR1}"

            results = yaml.load(yaml_config)
            assert results == {'FOO': "${VAR1}", 'BAR': [1, 2, 3]}
