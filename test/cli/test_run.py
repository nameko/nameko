import errno
import os
import signal
import socket

import eventlet
from mock import patch
import pytest

from nameko.cli.main import setup_parser
from nameko.cli.run import import_service, setup_backdoor, main, run
from nameko.exceptions import CommandError
from nameko.standalone.rpc import ClusterRpcProxy

from test.sample import Service


def test_run(rabbit_config):
    parser = setup_parser()
    broker = rabbit_config['AMQP_URI']
    args = parser.parse_args([
        'run',
        '--broker',
        broker,
        '--backdoor-port',
        0,
        'test.sample:Service',
    ])
    gt = eventlet.spawn(main, args)
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


def recv_until_prompt(sock):
    data = ""
    part = ""
    while not part.endswith('\n>>> '):
        part = sock.recv(4096)
        data += part
    return data


def test_backdoor():
    runner = object()
    green_socket, gt = setup_backdoor(runner, 0)
    eventlet.sleep(0)  # give backdoor a chance to spawn
    socket_name = green_socket.fd.getsockname()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(socket_name)
    recv_until_prompt(sock)  # banner

    sock.sendall("runner\n")
    runner_repr = recv_until_prompt(sock)
    assert str(runner) in runner_repr

    sock.sendall("quit()\n")
    error = recv_until_prompt(sock)
    assert 'RuntimeError: This would kill your service' in error
    sock.close()
    gt.kill()


def test_stopping(rabbit_config):
    with patch('nameko.cli.run.eventlet') as mock_eventlet:
        # this is the service "runlet"
        mock_eventlet.spawn().wait.side_effect = [
            KeyboardInterrupt,
            None,  # second wait, after stop() which returns normally
        ]
        gt = eventlet.spawn(run, [object], rabbit_config)
        gt.wait()
        # should complete


def test_stopping_twice(rabbit_config):
    with patch('nameko.cli.run.eventlet') as mock_eventlet:
        # this is the service "runlet"
        mock_eventlet.spawn().wait.side_effect = [
            KeyboardInterrupt,
            None,  # second wait, after stop() which returns normally
        ]
        with patch('nameko.cli.run.ServiceRunner') as runner_cls:
            runner = runner_cls()
            runner.stop.side_effect = KeyboardInterrupt
            runner.kill.return_value = None

            gt = eventlet.spawn(run, [object], rabbit_config)
            gt.wait()


def test_os_error_for_signal(rabbit_config):
    with patch('nameko.cli.run.eventlet') as mock_eventlet:
        # this is the service "runlet"
        mock_eventlet.spawn().wait.side_effect = [
            OSError(errno.EINTR, ''),
            None,  # second wait, after stop() which returns normally
        ]
        gt = eventlet.spawn(run, [object], rabbit_config)
        gt.wait()
        # should complete


def test_other_errors_propagate(rabbit_config):
    with patch('nameko.cli.run.eventlet') as mock_eventlet:
        # this is the service "runlet"
        mock_eventlet.spawn().wait.side_effect = [
            OSError(0, ''),
            None,  # second wait, after stop() which returns normally
        ]
        gt = eventlet.spawn(run, [object], rabbit_config)
        with pytest.raises(OSError):
            gt.wait()
