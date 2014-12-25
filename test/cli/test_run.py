import os
import signal
import socket
import sys

import eventlet
from mock import patch
import pytest

from nameko.cli import main
from nameko.cli.exceptions import CommandError
from nameko.cli.run import import_service, setup_backdoor
from nameko.standalone.rpc import ClusterRpcProxy

from test.sample import Service


def test_run(rabbit_config):
    broker = rabbit_config['AMQP_URI']
    with patch.object(
        sys,
        'argv',
        [
            'nameko',
            'run',
            '--broker',
            broker,
            '--backdoor-port',
            0,
            'test.sample:Service',
            ],
    ):
        gt = eventlet.spawn(main)
        eventlet.sleep(1)

        # make sure service launches ok
        with ClusterRpcProxy(rabbit_config) as proxy:
            proxy.service.ping()

        # stop service
        pid = os.getpid()
        os.kill(pid, signal.SIGTERM)
        gt.wait()


def test_import_ok():
    assert import_service('test.sample') is Service
    assert import_service('test.sample:Service') is Service


def test_import_missing():
    with pytest.raises(CommandError) as exc:
        import_service('non_existent')
    assert 'No module named non_existent' in str(exc)


def test_import_filename():
    with pytest.raises(CommandError) as exc:
        import_service('test/sample.py')
    assert "did you mean 'test.sample'?" in str(exc)


def test_import_broken():
    with pytest.raises(ImportError):
        import_service('test.broken_sample')


def test_import_missing_class():
    with pytest.raises(CommandError) as exc:
        import_service('test.sample:NonExistent')
    assert "Failed to find service class" in str(exc)


def test_import_not_a_class():
    with pytest.raises(CommandError) as exc:
        import_service('test.sample:rpc')
    assert "Service must be a class" in str(exc)


def test_backdoor():
    runner = object()
    green_socket = setup_backdoor(runner, 0)
    eventlet.sleep(1)
    socket_name = green_socket.fd.getsockname()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(socket_name)
    sock.recv(4096)  # banner

    sock.sendall("runner\n")
    runner_repr = sock.recv(4096)
    assert str(runner) in runner_repr

    sock.sendall("quit()\n")
    error = sock.recv(4096)
    assert 'RuntimeError: Do not call this. Unsafe' in error
    sock.close()
