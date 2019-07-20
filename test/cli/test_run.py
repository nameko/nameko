import errno
import os
import signal
import socket
from os.path import abspath, dirname, join
from textwrap import dedent

import eventlet
import pytest
from mock import call, patch

from nameko import config
from nameko.cli.run import run, setup_backdoor
from nameko.cli.utils import import_services
from nameko.constants import SERIALIZER_CONFIG_KEY, WEB_SERVER_CONFIG_KEY
from nameko.runners import ServiceRunner
from nameko.standalone.rpc import ClusterRpcClient
from nameko.testing.waiting import wait_for_call

from test.sample import Service


TEST_CONFIG_FILE = abspath(join(dirname(__file__), "config.yaml"))


@pytest.mark.usefixtures("rabbit_config")
def test_run(command):

    # start runner and wait for it to come up
    with wait_for_call(ServiceRunner, "start"):
        gt = eventlet.spawn(
            command, "nameko", "run", "--backdoor-port", "0", "test.sample:Service"
        )

    # make sure service launches ok
    with ClusterRpcClient() as client:
        client.service.ping()

    # stop service
    pid = os.getpid()
    os.kill(pid, signal.SIGTERM)
    gt.wait()


@pytest.mark.usefixtures("rabbit_config")
def test_main_with_config(command, tmpdir):

    config_file = tmpdir.join("config.yaml")
    config_file.write(
        """
        WEB_SERVER_ADDRESS: '0.0.0.0:8001'
        serializer: 'json'
    """
    )

    assert WEB_SERVER_CONFIG_KEY not in config
    assert SERIALIZER_CONFIG_KEY not in config

    with patch("nameko.cli.run.main") as run:

        command("nameko", "run", "--config", config_file.strpath, "test.sample")

        assert run.call_count == 1

    assert config[WEB_SERVER_CONFIG_KEY] == "0.0.0.0:8001"
    assert config[SERIALIZER_CONFIG_KEY] == "json"


@pytest.mark.usefixtures("rabbit_config")
def test_main_with_config_options(command, tmpdir):

    config_file = tmpdir.join("config.yaml")
    config_file.write(
        """
        WEB_SERVER_ADDRESS: '0.0.0.0:8001'
        serializer: 'json'
    """
    )

    assert WEB_SERVER_CONFIG_KEY not in config
    assert SERIALIZER_CONFIG_KEY not in config
    assert "EGG" not in config

    with patch("nameko.cli.run.main") as run:

        command(
            "nameko",
            "run",
            "--config",
            config_file.strpath,
            "--define",
            "serializer=pickle",
            "--define",
            'EGG=[{"spam": True}]',
            "test.sample",
        )

        assert run.call_count == 1

    assert config[WEB_SERVER_CONFIG_KEY] == "0.0.0.0:8001"
    assert config[SERIALIZER_CONFIG_KEY] == "pickle"
    assert config["EGG"] == [{"spam": True}]


@pytest.mark.usefixtures("rabbit_config")
def test_main_with_logging_config(command, tmpdir):

    config_content = """
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

    capture_file = tmpdir.join("capture.log")

    config_file = tmpdir.join("config.yaml")
    config_file.write(
        dedent(
            config_content.format(
                capture_file=capture_file.strpath, amqp_uri=config["AMQP_URI"]
            )
        )
    )

    # start runner and wait for it to come up
    with wait_for_call(ServiceRunner, "start"):
        gt = eventlet.spawn(
            command, "nameko", "run", "--config", config_file.strpath, "test.sample"
        )

    with ClusterRpcClient() as client:
        client.service.ping()

    pid = os.getpid()
    os.kill(pid, signal.SIGTERM)
    gt.wait()

    assert "test.sample - INFO - ping!" in capture_file.read()


def test_multiple_service_modules(command):

    with patch("nameko.cli.run.main") as run:

        command(
            "nameko",
            "run",
            "test.foo",
            "test.bar"
        )

        assert run.call_args_list == [call(("test.foo", "test.bar"), None)]


def test_parse_config_before_service_import(command, tmpdir, add_to_sys_path):

    config_file = tmpdir.join("config.yaml")
    config_file.write(dedent(
        """
        NAME: dynamic
        """
    ))

    service_file = tmpdir.join("service.py")
    service_file.write(dedent(
        """
        from nameko import config
        from nameko.testing.services import dummy

        class Service:
            name = config.get("NAME", "notfound")

            @dummy
            def method(self):
                return "OK"

        """
    ))

    service_names = []

    def cb(args, kwargs, res, exc_info):
        service_runner, = args
        service_names.extend(service_runner.service_names)
        return True

    with add_to_sys_path(tmpdir.strpath):

        with wait_for_call(ServiceRunner, "start", callback=cb):
            eventlet.spawn(
                command, "nameko", "run", "--config", config_file.strpath,
                "service:Service"
            )

    assert service_names == ["dynamic"]


def test_import_ok():
    assert import_services("test.sample") == [Service]
    assert import_services("test.sample:Service") == [Service]


def test_import_missing():
    with pytest.raises(ValueError) as exc:
        import_services("non_existent")
    assert "No module named" in str(exc.value)
    assert "non_existent" in str(exc.value)


def test_import_filename():
    with pytest.raises(ValueError) as exc:
        import_services("test/sample.py")
    assert "did you mean 'test.sample'?" in str(exc)


def test_import_broken():
    with pytest.raises(ValueError):
        import_services("test.broken_sample")


def test_import_missing_class():
    with pytest.raises(ValueError) as exc:
        import_services("test.sample:NonExistent")
    assert "Failed to find service class" in str(exc)


def test_import_not_a_class():
    with pytest.raises(ValueError) as exc:
        import_services("test.sample:rpc")
    assert "Service must be a class" in str(exc)


def test_import_no_service_classes():
    with pytest.raises(ValueError):
        import_services("test")


def recv_until_prompt(sock):
    data = b""
    part = b""
    while not data[-5:] == b"\n>>> ":
        part = sock.recv(4096)
        data += part
    return data


def test_backdoor():
    runner = object()
    green_socket, gt = setup_backdoor(runner, ("localhost", 0))
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
    assert "RuntimeError: This would kill your service" in str(error)
    sock.close()
    gt.kill()


def test_stopping(rabbit_config):
    with patch("nameko.cli.run.eventlet") as mock_eventlet:
        # this is the service "runlet"
        mock_eventlet.spawn().wait.side_effect = [
            KeyboardInterrupt,
            None,  # second wait, after stop() which returns normally
        ]
        gt = eventlet.spawn(run, ["test.sample"])
        gt.wait()
        # should complete


def test_stopping_twice(rabbit_config):
    with patch("nameko.cli.run.eventlet") as mock_eventlet:
        # this is the service "runlet"
        mock_eventlet.spawn().wait.side_effect = [
            KeyboardInterrupt,
            None,  # second wait, after stop() which returns normally
        ]
        with patch("nameko.cli.run.ServiceRunner") as runner_cls:
            runner = runner_cls()
            runner.stop.side_effect = KeyboardInterrupt
            runner.kill.return_value = None

            gt = eventlet.spawn(run, ["test.sample"])
            gt.wait()


def test_os_error_for_signal(rabbit_config):
    with patch("nameko.cli.run.eventlet") as mock_eventlet:
        # this is the service "runlet"
        mock_eventlet.spawn().wait.side_effect = [
            OSError(errno.EINTR, ""),
            None,  # second wait, after stop() which returns normally
        ]
        # don't actually start the service -- we're not firing a real signal
        # so the signal handler won't stop it again
        with patch.object(ServiceRunner, "start"):
            gt = eventlet.spawn(run, ["test.sample"])
            gt.wait()
        # should complete


def test_other_errors_propagate(rabbit_config):
    with patch("nameko.cli.run.eventlet") as mock_eventlet:
        # this is the service "runlet"
        mock_eventlet.spawn().wait.side_effect = [
            OSError(0, ""),
            None,  # second wait, after stop() which returns normally
        ]
        # don't actually start the service -- there's no real OSError that
        # would otherwise kill the whole process
        with patch.object(ServiceRunner, "start"):
            gt = eventlet.spawn(run, ["test.sample"])
            with pytest.raises(OSError):
                gt.wait()


@pytest.mark.usefixtures("empty_config")
def test_broker_option_deprecated(command, rabbit_uri):

    with patch("nameko.cli.run.run") as run:
        with patch("nameko.cli.utils.config.warnings") as warnings:
            command("nameko", "run", "--broker", rabbit_uri, "test.sample")

    assert run.call_count == 1

    assert warnings.warn.call_count
