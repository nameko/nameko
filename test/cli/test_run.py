import errno
import os
import signal
import socket
from os.path import join, dirname, abspath
import eventlet
from mock import patch
from textwrap import dedent
import pytest

from nameko.cli.main import setup_parser
from nameko.cli.run import import_service, setup_backdoor, main, run
from nameko.exceptions import CommandError
from nameko.runners import ServiceRunner
from nameko.standalone.rpc import ClusterRpcProxy
from nameko.constants import (
    AMQP_URI_CONFIG_KEY, WEB_SERVER_CONFIG_KEY, SERIALIZER_CONFIG_KEY)

from test.sample import Service


TEST_CONFIG_FILE = abspath(join(dirname(__file__), 'config.yaml'))


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


def test_main_with_config(rabbit_config):
    parser = setup_parser()
    args = parser.parse_args([
        'run',
        '--config',
        TEST_CONFIG_FILE,
        'test.sample',
    ])

    with patch('nameko.cli.run.run') as run:
        main(args)
        assert run.call_count == 1
        (_, config) = run.call_args[0]

        assert config == {
            WEB_SERVER_CONFIG_KEY: '0.0.0.0:8001',
            AMQP_URI_CONFIG_KEY: 'amqp://guest:guest@localhost',
            SERIALIZER_CONFIG_KEY: 'json'
        }


def test_main_with_logging_config(rabbit_config, tmpdir):

    config = """
        AMQP_URI: {amqp_uri}
        LOGGING:
            version: 1
            disable_existing_loggers: false
            formatters:
                simple:
                    format: "%(name)s - %(levelname)s - %(message)s"
            handlers:
                capture:
                    class: logging.FileHandler
                    level: INFO
                    formatter: simple
                    filename: {capture_file}
            root:
                level: INFO
                handlers: [capture]
    """

    capture_file = tmpdir.join('capture.log')

    config_file = tmpdir.join('config.yaml')
    config_file.write(
        dedent(config.format(
            capture_file=capture_file.strpath,
            amqp_uri=rabbit_config['AMQP_URI']
        ))
    )

    parser = setup_parser()
    args = parser.parse_args([
        'run',
        '--config',
        config_file.strpath,
        'test.sample',
    ])

    gt = eventlet.spawn(main, args)
    eventlet.sleep(1)

    with ClusterRpcProxy(rabbit_config) as proxy:
        proxy.service.ping()

    pid = os.getpid()
    os.kill(pid, signal.SIGTERM)
    gt.wait()

    assert "test.sample - INFO - ping!" in capture_file.read()


def test_import_ok():
    assert import_service('test.sample') == [Service]
    assert import_service('test.sample:Service') == [Service]


def test_import_missing():
    with pytest.raises(CommandError) as exc:
        import_service('non_existent')
    assert "No module named" in str(exc.value)
    assert "non_existent" in str(exc.value)


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


def test_import_no_service_classes():
    with pytest.raises(CommandError):
        import_service('test')


def recv_until_prompt(sock):
    data = b""
    part = b""
    while not data[-5:] == b'\n>>> ':
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

    sock.sendall(b"runner\n")
    runner_repr = recv_until_prompt(sock)
    assert repr(runner) in str(runner_repr)

    sock.sendall(b"quit()\n")
    error = recv_until_prompt(sock)
    assert 'RuntimeError: This would kill your service' in str(error)
    sock.close()
    gt.kill()


def test_stopping(rabbit_config):
    with patch('nameko.cli.run.eventlet') as mock_eventlet:
        # this is the service "runlet"
        mock_eventlet.spawn().wait.side_effect = [
            KeyboardInterrupt,
            None,  # second wait, after stop() which returns normally
        ]
        gt = eventlet.spawn(run, [Service], rabbit_config)
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

            gt = eventlet.spawn(run, [Service], rabbit_config)
            gt.wait()


def test_os_error_for_signal(rabbit_config):
    with patch('nameko.cli.run.eventlet') as mock_eventlet:
        # this is the service "runlet"
        mock_eventlet.spawn().wait.side_effect = [
            OSError(errno.EINTR, ''),
            None,  # second wait, after stop() which returns normally
        ]
        # don't actually start the service -- we're not firing a real signal
        # so the signal handler won't stop it again
        with patch.object(ServiceRunner, 'start'):
            gt = eventlet.spawn(run, [Service], rabbit_config)
            gt.wait()
        # should complete


def test_other_errors_propagate(rabbit_config):
    with patch('nameko.cli.run.eventlet') as mock_eventlet:
        # this is the service "runlet"
        mock_eventlet.spawn().wait.side_effect = [
            OSError(0, ''),
            None,  # second wait, after stop() which returns normally
        ]
        # don't actually start the service -- there's no real OSError that
        # would otherwise kill the whole process
        with patch.object(ServiceRunner, 'start'):
            gt = eventlet.spawn(run, [Service], rabbit_config)
            with pytest.raises(OSError):
                gt.wait()
