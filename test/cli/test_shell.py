import os
import sys

from mock import patch, Mock
import pytest

from nameko.standalone.rpc import ClusterProxy
from nameko.cli.main import setup_parser
from nameko.cli.shell import make_nameko_helper, main


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


def test_basic(pystartup):
    parser = setup_parser()
    args = parser.parse_args(['shell'])

    with patch('nameko.cli.shell.code') as code:
        main(args)

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


def test_plain(pystartup):
    parser = setup_parser()
    args = parser.parse_args(['shell', '--interface', 'plain'])

    with patch('nameko.cli.shell.code') as code:
        main(args)

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


def test_plain_fallback(pystartup):
    parser = setup_parser()
    args = parser.parse_args(['shell', '--interface', 'bpython'])

    with patch('nameko.cli.shell.code') as code:
        main(args)

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


def test_bpython(pystartup):
    parser = setup_parser()
    args = parser.parse_args(['shell', '--interface', 'bpython'])

    with patch('bpython.embed') as embed:
        main(args)

    _, kwargs = embed.call_args
    local = kwargs['locals_']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


def test_ipython(pystartup):
    parser = setup_parser()
    args = parser.parse_args(['shell', '--interface', 'ipython'])

    with patch('IPython.embed') as embed:
        main(args)

    _, kwargs = embed.call_args
    local = kwargs['user_ns']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()
