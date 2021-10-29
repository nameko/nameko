import os
import sys
from types import ModuleType

from nameko import config
from nameko.constants import AMQP_URI_CONFIG_KEY
from nameko.standalone.events import event_dispatcher
from nameko.standalone.rpc import ClusterRpcClient

from .utils import interact


SHELLS = ["bpython", "ipython", "plain"]


class ShellRunner(object):
    def __init__(self, banner, local):
        self.banner = banner
        self.local = local

    def bpython(self):
        import bpython  # pylint: disable=E0401

        bpython.embed(banner=self.banner, locals_=self.local)

    def ipython(self):
        from IPython import embed  # pylint: disable=E0401

        embed(banner1=self.banner, user_ns=self.local)

    def plain(self):
        interact(
            banner=self.banner,
            local=self.local,
            raise_expections=not sys.stdin.isatty(),
        )

    def start_shell(self, name):
        if not sys.stdin.isatty():
            available_shells = ["plain"]
        else:
            available_shells = [name] if name else SHELLS

        # Support the regular Python interpreter startup script if someone
        # is using it.
        startup = os.environ.get("PYTHONSTARTUP")
        if startup and os.path.isfile(startup):
            with open(startup, "r") as f:
                eval(compile(f.read(), startup, "exec"), self.local)
            del os.environ["PYTHONSTARTUP"]

        for name in available_shells:
            try:
                return getattr(self, name)()
            except ImportError:  # pragma: no cover noqa: F401
                pass
        self.plain()  # pragma: no cover


def make_nameko_helper():
    """Create a fake module that provides some convenient access to nameko
    standalone functionality for interactive shell usage.
    """
    module = ModuleType("nameko")
    module.__doc__ = """Nameko shell helper for making rpc calls and dispatching
events.

Usage:
    >>> n.rpc.service.method()
    "reply"

    >>> n.dispatch_event('service', 'event_type', 'event_data')
"""
    client = ClusterRpcClient()
    module.rpc = client.start()
    module.dispatch_event = event_dispatcher()
    module.config = config
    module.disconnect = client.stop
    return module


def main(interface):

    banner = "Nameko Python %s shell on %s\nBroker: %s" % (
        sys.version,
        sys.platform,
        config[AMQP_URI_CONFIG_KEY],
    )

    ctx = {}
    ctx["n"] = make_nameko_helper()

    runner = ShellRunner(banner, ctx)
    runner.start_shell(name=interface)
