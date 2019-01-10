import os
import subprocess
import sys

import pytest
from mock import Mock, patch

from nameko.cli.commands import Shell
from nameko.cli.main import setup_parser
from nameko.cli.shell import make_nameko_helper
from nameko.constants import (
    AMQP_URI_CONFIG_KEY, SERIALIZER_CONFIG_KEY, WEB_SERVER_CONFIG_KEY
)
from nameko.standalone.rpc import ClusterProxy


def test_helper_module(rabbit_config):
    helper = make_nameko_helper(rabbit_config)
    assert isinstance(helper.rpc, ClusterProxy)
    helper.disconnect()


@pytest.yield_fixture
def pystartup(tmpdir):
    startup = tmpdir.join('startup.py')
    startup.write('foo = 42')

    with patch.dict(os.environ, {'PYTHONSTARTUP': str(startup)}):
        yield


@pytest.yield_fixture(autouse=True)
def fake_alternative_interpreters():
    # Make sure these appear unavailable even if installed. We cheat slightly,
    # and have the call to `embed` raise the ImportError, rather than the
    # actual module import (this is easier to do and has the same effect in our
    # case).
    fake_module = Mock()
    fake_module.embed.side_effect = ImportError
    with patch.dict(sys.modules, {
        'IPython': fake_module,
        'bpython': fake_module,
    }):
        yield


@pytest.fixture
def isatty():
    with patch('nameko.cli.shell.sys.stdin.isatty') as isatty:
        yield isatty


def test_basic(pystartup, rabbit_config):
    parser = setup_parser()
    args = parser.parse_args([
        'shell', '--broker', rabbit_config[AMQP_URI_CONFIG_KEY]
    ])

    with patch("nameko.cli.shell.code") as code:
        Shell.main(args)

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


def test_plain(pystartup, rabbit_config):
    parser = setup_parser()
    args = parser.parse_args([
        'shell', '--broker', rabbit_config[AMQP_URI_CONFIG_KEY],
        '--interface', 'plain'
    ])

    with patch('nameko.cli.shell.code') as code:
        Shell.main(args)

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


def test_plain_fallback(pystartup, rabbit_config):
    parser = setup_parser()
    args = parser.parse_args([
        'shell', '--broker', rabbit_config[AMQP_URI_CONFIG_KEY],
        '--interface', 'bpython'
    ])

    with patch('nameko.cli.shell.code') as code:
        Shell.main(args)

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


def test_bpython(pystartup, rabbit_config, isatty):
    parser = setup_parser()
    args = parser.parse_args([
        'shell', '--broker', rabbit_config[AMQP_URI_CONFIG_KEY],
        '--interface', 'bpython'
    ])

    isatty.return_value = True

    with patch('bpython.embed') as embed:
        Shell.main(args)

    _, kwargs = embed.call_args
    local = kwargs['locals_']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


def test_ipython(pystartup, rabbit_config, isatty):
    parser = setup_parser()
    args = parser.parse_args([
        'shell', '--broker', rabbit_config[AMQP_URI_CONFIG_KEY],
        '--interface', 'ipython'
    ])

    isatty.return_value = True

    with patch('IPython.embed') as embed:
        Shell.main(args)

    _, kwargs = embed.call_args
    local = kwargs['user_ns']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


def test_uses_plain_when_not_tty(pystartup, rabbit_config, isatty):
    parser = setup_parser()
    args = parser.parse_args([
        'shell', '--broker', rabbit_config[AMQP_URI_CONFIG_KEY],
        '--interface', 'ipython'
    ])

    isatty.return_value = False

    with patch('nameko.cli.shell.code') as code:
        Shell.main(args)

    assert code.interact.called


def test_config(pystartup, rabbit_config, tmpdir):

    config = tmpdir.join('config.yaml')
    config.write("""
        WEB_SERVER_ADDRESS: '0.0.0.0:8001'
        AMQP_URI: '{}'
        serializer: 'json'
    """.format(rabbit_config[AMQP_URI_CONFIG_KEY]))

    parser = setup_parser()
    args = parser.parse_args(['shell', '--config', config.strpath])

    with patch('nameko.cli.shell.code') as code:
        Shell.main(args)

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['n'].config == {
        WEB_SERVER_CONFIG_KEY: '0.0.0.0:8001',
        AMQP_URI_CONFIG_KEY: rabbit_config[AMQP_URI_CONFIG_KEY],
        SERIALIZER_CONFIG_KEY: 'json'
    }
    local['n'].disconnect()


class TestBanner(object):

    @pytest.yield_fixture(autouse=True)
    def patch_nameko_helper(self):
        with patch('nameko.cli.shell.make_nameko_helper'):
            yield

    def test_broker_as_param(self):

        amqp_uri = "amqp://broker/param"

        parser = setup_parser()
        args = parser.parse_args(['shell', '--broker', amqp_uri])

        with patch('nameko.cli.shell.ShellRunner') as shell_runner:
            Shell.main(args)

        expected_message = (
            "Broker: {}".format(amqp_uri)
        )
        (banner, _), _ = shell_runner.call_args
        assert expected_message in banner

    def test_broker_from_config(self, tmpdir):

        amqp_uri = "amqp://broker/config"

        config = tmpdir.join('config.yaml')
        config.write("""
            WEB_SERVER_ADDRESS: '0.0.0.0:8001'
            AMQP_URI: '{}'
            serializer: 'json'
        """.format(amqp_uri))

        parser = setup_parser()
        args = parser.parse_args(['shell', '--config', config.strpath])

        with patch('nameko.cli.shell.ShellRunner') as shell_runner:
            Shell.main(args)

        expected_message = (
            "Broker: {} (from --config)".format(amqp_uri)
        )
        (banner, _), _ = shell_runner.call_args
        assert expected_message in banner


class TestExitCode:

    def test_exit_code_0(self):
        """ Test ``echo print(1) | nameko shell``

        This command should return an exit code of 0.
        """

        echo_process = subprocess.Popen(
            ("echo", "print(1)"), stdout=subprocess.PIPE
        )
        nameko_process = subprocess.Popen(
            ("nameko", "shell"), stdin=echo_process.stdout
        )
        nameko_process.wait()
        assert nameko_process.returncode == 0

    def test_exit_code_1(self):
        """ Test ``echo raise Exception() | nameko shell``

        This command should return an exit code of 1.
        """

        echo_process = subprocess.Popen(
            ("echo", "raise Exception()"), stdout=subprocess.PIPE
        )
        nameko_process = subprocess.Popen(
            ("nameko", "shell"), stdin=echo_process.stdout
        )
        nameko_process.wait()
        assert nameko_process.returncode == 1
