import sys
from mock import patch, Mock

from nameko.standalone.rpc import ClusterProxy
from nameko.cli.main import setup_parser
from nameko.cli.shell import make_nameko_helper, main


def test_helper_module(rabbit_config):
    helper = make_nameko_helper(rabbit_config)
    assert isinstance(helper.rpc, ClusterProxy)
    helper.disconnect()


def test_basic(tmpdir):
    parser = setup_parser()
    args = parser.parse_args(['shell'])

    startup = tmpdir.join('startup.py')
    startup.write('foo = 42')

    with patch('nameko.cli.shell.os.environ') as environ:
        environ.get.return_value = str(startup)
        with patch('nameko.cli.shell.code') as code:
            main(args)

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


def test_plain(tmpdir):
    parser = setup_parser()
    args = parser.parse_args(['shell', '--interface', 'plain'])

    startup = tmpdir.join('startup.py')
    startup.write('foo = 42')

    with patch('nameko.cli.shell.os.environ') as environ:
        environ.get.return_value = str(startup)
        with patch('nameko.cli.shell.code') as code:
            main(args)

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


def test_plain_fallback(tmpdir):
    parser = setup_parser()
    args = parser.parse_args(['shell', '--interface', 'bpython'])

    startup = tmpdir.join('startup.py')
    startup.write('foo = 42')

    with patch('nameko.cli.shell.os.environ') as environ:
        environ.get.return_value = str(startup)
        with patch('nameko.cli.shell.code') as code:
            main(args)

    _, kwargs = code.interact.call_args
    local = kwargs['local']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


def test_bpython(tmpdir):
    parser = setup_parser()
    args = parser.parse_args(['shell', '--interface', 'bpython'])

    startup = tmpdir.join('startup.py')
    startup.write('foo = 42')

    sys.modules['bpython'] = Mock()

    with patch('nameko.cli.shell.os.environ') as environ:
        environ.get.return_value = str(startup)
        with patch('bpython.embed') as embed:
            main(args)

    _, kwargs = embed.call_args
    local = kwargs['locals_']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()


def test_ipython(tmpdir):
    parser = setup_parser()
    args = parser.parse_args(['shell', '--interface', 'ipython'])

    startup = tmpdir.join('startup.py')
    startup.write('foo = 42')

    sys.modules['IPython'] = Mock()

    with patch('nameko.cli.shell.os.environ') as environ:
        environ.get.return_value = str(startup)
        with patch('IPython.embed') as embed:
            main(args)

    _, kwargs = embed.call_args
    local = kwargs['user_ns']
    assert 'n' in local.keys()
    assert local['foo'] == 42
    local['n'].disconnect()
