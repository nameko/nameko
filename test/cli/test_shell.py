import os
import sys

import pytest
from mock import Mock, patch

from nameko.cli.shell import make_nameko_helper
from nameko.constants import (
    AMQP_URI_CONFIG_KEY, SERIALIZER_CONFIG_KEY, WEB_SERVER_CONFIG_KEY
)
from nameko.rpc import Client


def test_helper_module(rabbit_config):
    helper = make_nameko_helper()
    assert isinstance(helper.rpc, Client)
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


@pytest.mark.usefixtures('rabbit_config')
def test_basic(command, pystartup):
    with patch('nameko.cli.shell.code') as code:
        command('nameko', 'shell')

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


@pytest.mark.usefixtures('rabbit_config')
def test_plain(command, pystartup):
    with patch('nameko.cli.shell.code') as code:
        command(
            'nameko', 'shell',
            '--interface', 'plain'
        )

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


@pytest.mark.usefixtures('rabbit_config')
def test_plain_fallback(command, pystartup):
    with patch('nameko.cli.shell.code') as code:
        command(
            'nameko', 'shell',
            '--interface', 'bpython'
        )

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


@pytest.mark.usefixtures('rabbit_config')
def test_bpython(command, pystartup):
    with patch('bpython.embed') as embed:
        command(
            'nameko', 'shell',
            '--interface', 'bpython'
        )

    _, kwargs = embed.call_args
    local = kwargs['locals_']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


@pytest.mark.usefixtures('rabbit_config')
def test_ipython(command, pystartup):
    with patch('IPython.embed') as embed:
        command(
            'nameko', 'shell',
            '--interface', 'ipython'
        )

    _, kwargs = embed.call_args
    local = kwargs['user_ns']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


@pytest.mark.usefixtures('rabbit_config')
def test_config(command, pystartup, tmpdir):

    config_file = tmpdir.join('config.yaml')
    config_file.write("""
        WEB_SERVER_ADDRESS: '0.0.0.0:8001'
        serializer: 'json'
    """)

    with patch('nameko.cli.shell.code') as code:
        command(
            'nameko', 'shell',
            '--config', config_file.strpath,
        )

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['n'].config[WEB_SERVER_CONFIG_KEY] == '0.0.0.0:8001'
    assert local['n'].config[SERIALIZER_CONFIG_KEY] == 'json'
    local['n'].disconnect()


@pytest.mark.usefixtures('rabbit_config')
def test_config_options(command, pystartup, tmpdir):

    config_file = tmpdir.join('config.yaml')
    config_file.write("""
        WEB_SERVER_ADDRESS: '0.0.0.0:8001'
        serializer: 'json'
    """)

    with patch('nameko.cli.shell.code') as code:
        command(
            'nameko', 'shell',
            '--config', config_file.strpath,
            '--define', 'serializer=pickle',
            '--define', 'EGG=[{"spam": True}]',
        )

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['n'].config[WEB_SERVER_CONFIG_KEY] == '0.0.0.0:8001'
    assert local['n'].config[SERIALIZER_CONFIG_KEY] == 'pickle'
    assert local['n'].config['EGG'] == [{'spam': True}]
    local['n'].disconnect()


@pytest.mark.usefixtures('empty_config')
class TestBanner(object):

    @pytest.yield_fixture(autouse=True)
    def patch_nameko_helper(self):
        with patch('nameko.cli.shell.make_nameko_helper'):
            yield

    def test_banner(self, command, tmpdir):

        amqp_uri = "amqp://broker/config"

        config_file = tmpdir.join('config.yaml')
        config_file.write("""
            WEB_SERVER_ADDRESS: '0.0.0.0:8001'
            AMQP_URI: '{}'
            serializer: 'json'
        """.format(amqp_uri))

        with patch('nameko.cli.shell.ShellRunner') as shell_runner:
            command(
                'nameko', 'shell',
                '--config', config_file.strpath
            )

        expected_message = (
            "Broker: {}".format(amqp_uri)
        )
        (banner, _), _ = shell_runner.call_args
        assert expected_message in banner


@pytest.mark.usefixtures('empty_config')
def test_broker_option_deprecated(command, rabbit_uri):

    with patch('nameko.cli.shell.code') as code:
        with patch('nameko.cli.main.warnings') as warnings:
            command(
                'nameko', 'shell',
                '--broker', rabbit_uri,
            )

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['n'].config[AMQP_URI_CONFIG_KEY] == rabbit_uri
    local['n'].disconnect()

    assert warnings.warn.call_count
