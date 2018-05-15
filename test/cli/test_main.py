import os
import sys

import pytest
import yaml
from mock import patch

from nameko.cli.main import (
    ENV_VAR_MATCHER, main, setup_parser, setup_yaml_parser
)
from nameko.exceptions import CommandError, ConfigurationError


try:
    import regex
except ImportError:  # pragma: no cover
    has_regex_module = False
else:  # pragma: no cover
    del regex
    has_regex_module = True


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


class TestConfigEnvironmentParser(object):

    @pytest.mark.parametrize(('value', 'expected'), [
        # no var
        ('raw', []),
        # var without default
        ('${VAR_NAME}', [('VAR_NAME', '')]),
        # on var with default value
        ('${VAR_NAME:default}', [('VAR_NAME', 'default')]),
        # multiple match with default
        ('${VAR_NAME1:default1}_${VAR_NAME2:default2}', [
            ('VAR_NAME1', 'default1'),
            ('VAR_NAME2', 'default2'),
        ]),
    ])
    def test_maching_env_variable(self, value, expected):
        res = ENV_VAR_MATCHER.findall(value)
        assert res == expected

    @pytest.mark.parametrize(('value', 'expected'), [
        # on var, with {} in default (as str.format accept)
        ('${VAR_NAME:my-{pytemplate}-template}', [
            ('VAR_NAME', 'my-{pytemplate}-template')]),
        # var with var in default
        ('${VAR_NAME:my-${OTHER_VAR}-template}', [
            ('VAR_NAME', 'my-${OTHER_VAR}-template')]),
        # recursive interpretation of last var
        ('my-${OTHER_VAR}-template', [
            ('OTHER_VAR', '')]),
        # var with var in default in default
        ('${VAR_NAME:my-${OTHER_VAR:1}-template}', [
            ('VAR_NAME', 'my-${OTHER_VAR:1}-template')]),
        # recursive interpretation of last var
        ('my-${OTHER_VAR:1}-template', [
            ('OTHER_VAR', '1')]),
        # multiple var in default
        ('${VAR_NAME1:default1}_${VAR_NAME2:default2}', [
            ('VAR_NAME1', 'default1'),
            ('VAR_NAME2', 'default2'),
        ]),
    ])
    @pytest.mark.skipif(not has_regex_module,
                        reason='0 support for nested env without regex module')
    def test_maching_recursive_with_regex(
            self, value, expected
    ):  # pragma: no cover
        res = ENV_VAR_MATCHER.findall(value)
        assert res == expected


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
        ),
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

    @pytest.mark.parametrize(('yaml_config', 'env_vars', 'expected_config'), [
        # recursive env with root value
        (
            """
            FOO: ${FOO:val_${INDICE:1}}
            """,
            {"FOO": 'val_a'},
            {"FOO": 'val_a'},
        ),
        # recursive env with root default and sub value
        (
            """
            FOO: ${FOO:val_${INDICE:1}}
            """,
            {"INDICE": 'b'},
            {"FOO": 'val_b'},
        ),
        # recursive env requiring data
        (
            """
            FOO: ${FOO:val_${INDICE}}
            """,
            {"INDICE": 'b'},
            {"FOO": 'val_b'},
        ),

        # default data with bracket
        (
            """
            FOO: ${FOO:"{name} {age}"}
            """,
            {},
            {"FOO": '{name} {age}'},
        ),
        # default data with bracket
        (
            """
            FOO: ${FOO:"{name} {age}"}
            """,
            {"FOO": '"{surname}"'},
            {"FOO": '{surname}'},
        ),
    ])
    @pytest.mark.skipif(not has_regex_module,
                        reason='0 support for nested env without regex module')
    def test_environment_vars_recursive_in_config(
            self, yaml_config, env_vars, expected_config
    ):  # pragma: no cover
        setup_yaml_parser()

        with patch.dict('os.environ'):
            for key, val in env_vars.items():
                os.environ[key] = val

            results = yaml.load(yaml_config)
            assert results == expected_config

    @pytest.mark.parametrize(('yaml_config', 'env_vars', 'expected_config'), [
        # recursive env with root value
        (
            """
            FOO: ${FOO:val_${INDICE:1}}
            """,
            {"FOO": 'val_a'},
            {"FOO": 'val_a}'},
        ),
        # recursive env with root default and sub value
        (
            """
            FOO: ${FOO:val_${INDICE:1}}
            """,
            {"INDICE": 'b'},
            {"FOO": 'val_${INDICE:1}'},
        ),
        # recursive env requiring data
        (
            """
            FOO: ${FOO:${INDICE}_val}
            """,
            {"INDICE": 'b'},
            {"FOO": '${INDICE_val}'},
        ),

    ])
    @pytest.mark.skipif(has_regex_module,
                        reason='default behavior if no regex module')
    def test_unhandled_recursion(
            self, yaml_config, env_vars, expected_config
    ):  # pragma: no cover
        setup_yaml_parser()

        with patch.dict('os.environ'):
            for key, val in env_vars.items():
                os.environ[key] = val
            results = yaml.load(yaml_config)
            assert results == expected_config
