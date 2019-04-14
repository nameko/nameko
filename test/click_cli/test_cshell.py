import os
import subprocess
import sys

import pytest
from mock import Mock, patch

from nameko.click_cli.do_shell import make_nameko_helper
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
    startup = tmpdir.join("startup.py")
    startup.write("foo = 42")

    with patch.dict(os.environ, {"PYTHONSTARTUP": str(startup)}):
        yield


@pytest.yield_fixture(autouse=True)
def fake_alternative_interpreters():
    # Make sure these appear unavailable even if installed. We cheat slightly,
    # and have the call to `embed` raise the ImportError, rather than the
    # actual module import (this is easier to do and has the same effect in our
    # case).
    fake_module = Mock()
    fake_module.embed.side_effect = ImportError
    with patch.dict(sys.modules, {"IPython": fake_module, "bpython": fake_module}):
        yield


@pytest.fixture
def isatty():
    with patch("nameko.click_cli.do_shell.sys.stdin.isatty") as isatty:
        yield isatty


@pytest.mark.usefixtures("rabbit_config")
def test_basic(command, pystartup):
    with patch("nameko.click_cli.do_shell.interact") as interact:
        command('cnameko', 'shell')

    _, kwargs = interact.call_args
    local = kwargs["local"]
    assert "n" in local.keys()
    assert local["foo"] == 42
    local["n"].disconnect()


@pytest.mark.usefixtures("rabbit_config")
def test_plain(command, pystartup):
    with patch("nameko.click_cli.do_shell.interact") as interact:
        command("cnameko", "shell", "--interface", "plain")

    _, kwargs = interact.call_args
    local = kwargs["local"]
    assert "n" in local.keys()
    assert local["foo"] == 42
    local["n"].disconnect()


@pytest.mark.usefixtures("rabbit_config")
def test_plain_fallback(command, pystartup):
    with patch("nameko.click_cli.do_shell.interact") as interact:
        command("cnameko", "shell", "--interface", "bpython")

    _, kwargs = interact.call_args
    local = kwargs["local"]
    assert "n" in local.keys()
    assert local["foo"] == 42
    local["n"].disconnect()


@pytest.mark.usefixtures("rabbit_config")
def test_bpython(command, pystartup, isatty):

    isatty.return_value = True

    with patch("bpython.embed") as embed:
        command("cnameko", "shell", "--interface", "bpython")

    _, kwargs = embed.call_args
    local = kwargs["locals_"]
    assert "n" in local.keys()
    assert local["foo"] == 42
    local["n"].disconnect()


@pytest.mark.usefixtures("rabbit_config")
def test_ipython(command, pystartup, isatty):

    isatty.return_value = True

    with patch("IPython.embed") as embed:
        command("cnameko", "shell", "--interface", "ipython")

    _, kwargs = embed.call_args
    local = kwargs["user_ns"]
    assert "n" in local.keys()
    assert local["foo"] == 42
    local["n"].disconnect()


@pytest.mark.usefixtures("rabbit_config")
def test_uses_plain_when_not_tty(command, pystartup, isatty):

    isatty.return_value = False

    with patch("nameko.click_cli.do_shell.interact") as interact:
        command("cnameko", "shell", "--interface", "ipython")

    assert interact.called


@pytest.mark.usefixtures("rabbit_config")
def test_config(command, pystartup, tmpdir):

    config_file = tmpdir.join("config.yaml")
    config_file.write(
        """
        WEB_SERVER_ADDRESS: '0.0.0.0:8001'
        serializer: 'json'
    """
    )

    with patch("nameko.click_cli.do_shell.interact") as interact:
        command("cnameko", "shell", "--config", config_file.strpath)

    _, kwargs = interact.call_args
    local = kwargs["local"]
    assert "n" in local.keys()
    assert local["n"].config[WEB_SERVER_CONFIG_KEY] == "0.0.0.0:8001"
    assert local["n"].config[SERIALIZER_CONFIG_KEY] == "json"
    local["n"].disconnect()


@pytest.mark.usefixtures("rabbit_config")
def test_config_options(command, pystartup, tmpdir):

    config_file = tmpdir.join("config.yaml")
    config_file.write(
        """
        WEB_SERVER_ADDRESS: '0.0.0.0:8001'
        serializer: 'json'
    """
    )

    with patch("nameko.click_cli.do_shell.interact") as interact:
        command(
            "cnameko",
            "shell",
            "--config",
            config_file.strpath,
            "--define",
            "serializer=pickle",
            "--define",
            'EGG=[{"spam": True}]',
        )

    _, kwargs = interact.call_args
    local = kwargs["local"]
    assert "n" in local.keys()
    assert local["n"].config[WEB_SERVER_CONFIG_KEY] == "0.0.0.0:8001"
    assert local["n"].config[SERIALIZER_CONFIG_KEY] == "pickle"
    assert local["n"].config["EGG"] == [{"spam": True}]
    local["n"].disconnect()


@pytest.mark.usefixtures("empty_config")
class TestBanner(object):
    @pytest.yield_fixture(autouse=True)
    def patch_nameko_helper(self):
        with patch("nameko.click_cli.do_shell.make_nameko_helper"):
            yield

    def test_banner(self, command, tmpdir):

        amqp_uri = "amqp://broker/config"

        config_file = tmpdir.join("config.yaml")
        config_file.write(
            """
            WEB_SERVER_ADDRESS: '0.0.0.0:8001'
            AMQP_URI: '{}'
            serializer: 'json'
        """.format(
                amqp_uri
            )
        )

        with patch("nameko.click_cli.do_shell.ShellRunner") as shell_runner:
            command("cnameko", "shell", "--config", config_file.strpath)

        expected_message = "Broker: {}".format(amqp_uri)
        (banner, _), _ = shell_runner.call_args
        assert expected_message in banner


@pytest.mark.usefixtures("empty_config")
def test_broker_option_deprecated(command, rabbit_uri):

    with patch("nameko.click_cli.do_shell.interact") as interact:
        with patch("nameko.click_cli.utils.config.warnings") as warnings:
            command("cnameko", "shell", "--broker", rabbit_uri)

    _, kwargs = interact.call_args
    local = kwargs["local"]
    assert "n" in local.keys()
    assert local["n"].config[AMQP_URI_CONFIG_KEY] == rabbit_uri
    local["n"].disconnect()

    assert warnings.warn.call_count


class TestExitCode:
    def test_exit_code_0(self):
        """ Test ``echo print(1) | nameko shell``

        This command should return an exit code of 0.
        """

        echo_process = subprocess.Popen(("echo", "print(1)"), stdout=subprocess.PIPE)
        nameko_process = subprocess.Popen(
            ("cnameko", "shell", "--define=AMQP_URI=memory://localhost"),
            stdin=echo_process.stdout,
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
            ("cnameko", "shell", "--define=AMQP_URI=memory://localhost"),
            stdin=echo_process.stdout,
        )
        nameko_process.wait()
        assert nameko_process.returncode == 1
