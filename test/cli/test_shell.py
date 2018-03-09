import os
import sys

import pytest
from mock import Mock, patch

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


def test_basic(command, rabbit_config, pystartup):
    with patch('nameko.cli.shell.code') as code:
        command(
            'nameko', 'shell',
            '--broker', rabbit_config[AMQP_URI_CONFIG_KEY]
        )

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


def test_plain(command, rabbit_config, pystartup):
    with patch('nameko.cli.shell.code') as code:
        command(
            'nameko', 'shell',
            '--broker', rabbit_config[AMQP_URI_CONFIG_KEY],
            '--interface', 'plain'
        )

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


def test_plain_fallback(command, rabbit_config, pystartup):
    with patch('nameko.cli.shell.code') as code:
        command(
            'nameko', 'shell',
            '--broker', rabbit_config[AMQP_URI_CONFIG_KEY],
            '--interface', 'bpython'
        )

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


def test_bpython(command, rabbit_config, pystartup):
    with patch('bpython.embed') as embed:
        command(
            'nameko', 'shell',
            '--broker', rabbit_config[AMQP_URI_CONFIG_KEY],
            '--interface', 'bpython'
        )

    _, kwargs = embed.call_args
    local = kwargs['locals_']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


def test_ipython(command, rabbit_config, pystartup):
    with patch('IPython.embed') as embed:
        command(
            'nameko', 'shell',
            '--broker', rabbit_config[AMQP_URI_CONFIG_KEY],
            '--interface', 'ipython'
        )

    _, kwargs = embed.call_args
    local = kwargs['user_ns']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


def test_config(command, pystartup, rabbit_config, tmpdir):

    config = tmpdir.join('config.yaml')
    config.write("""
        WEB_SERVER_ADDRESS: '0.0.0.0:8001'
        AMQP_URI: '{}'
        serializer: 'json'
    """.format(rabbit_config[AMQP_URI_CONFIG_KEY]))

    with patch('nameko.cli.shell.code') as code:
        command(
            'nameko', 'shell',
            '--config', config.strpath,
        )

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['n'].config == {
        WEB_SERVER_CONFIG_KEY: '0.0.0.0:8001',
        AMQP_URI_CONFIG_KEY: rabbit_config[AMQP_URI_CONFIG_KEY],
        SERIALIZER_CONFIG_KEY: 'json'
    }
    local['n'].disconnect()


def test_config_options(command, pystartup, rabbit_config, tmpdir):

    config = tmpdir.join('config.yaml')
    config.write("""
        WEB_SERVER_ADDRESS: '0.0.0.0:8001'
        AMQP_URI: '{}'
        serializer: 'json'
    """.format(rabbit_config[AMQP_URI_CONFIG_KEY]))

    with patch('nameko.cli.shell.code') as code:
        command(
            'nameko', 'shell',
            '--config', config.strpath,
            '--define', 'serializer=pickle',
            '--define', 'EGG=[{"spam": True}]',
        )

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['n'].config == {
        WEB_SERVER_CONFIG_KEY: '0.0.0.0:8001',
        AMQP_URI_CONFIG_KEY: rabbit_config[AMQP_URI_CONFIG_KEY],
        SERIALIZER_CONFIG_KEY: 'pickle',  # overrides config file value
        'EGG': [{'spam': True}],
    }
    local['n'].disconnect()


class TestBanner(object):

    @pytest.yield_fixture(autouse=True)
    def patch_nameko_helper(self):
        with patch('nameko.cli.shell.make_nameko_helper'):
            yield

    def test_broker_as_param(self, command):

        amqp_uri = "amqp://broker/param"

        with patch('nameko.cli.shell.ShellRunner') as shell_runner:
            command(
                'nameko', 'shell',
                '--broker', amqp_uri,
            )

        expected_message = (
            "Broker: {}".format(amqp_uri)
        )
        (banner, _), _ = shell_runner.call_args
        assert expected_message in banner

    def test_broker_from_config(self, command, tmpdir):

        amqp_uri = "amqp://broker/config"

        config = tmpdir.join('config.yaml')
        config.write("""
            WEB_SERVER_ADDRESS: '0.0.0.0:8001'
            AMQP_URI: '{}'
            serializer: 'json'
        """.format(amqp_uri))

        with patch('nameko.cli.shell.ShellRunner') as shell_runner:
            command(
                'nameko', 'shell',
                '--config', config.strpath
            )

        expected_message = (
            "Broker: {} (from --config)".format(amqp_uri)
        )
        (banner, _), _ = shell_runner.call_args
        assert expected_message in banner
