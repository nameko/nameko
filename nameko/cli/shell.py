import os
import sys
from types import ModuleType

import yaml

import nameko.cli.code as code
from nameko.constants import AMQP_URI_CONFIG_KEY

from .commands import Shell


class ShellRunner(object):

    def __init__(self, banner, get_local):
        self.banner = banner
        self._get_local = get_local

    def bpython(self):
        import bpython  # pylint: disable=E0401
        bpython.embed(banner=self.banner, locals_=self.local())

    def ipython(self):
        from IPython import embed  # pylint: disable=E0401
        embed(banner1=self.banner, user_ns=self.local())

    def plain(self):
        code.interact(
            banner=self.banner,
            local=self.local(),
            raise_expections=not sys.stdin.isatty()
        )

    def local(self):
        local = self._get_local()
        # Support the regular Python interpreter startup script if someone
        # is using it.
        startup = os.environ.get('PYTHONSTARTUP')
        if startup and os.path.isfile(startup):
            with open(startup, 'r') as f:
                eval(compile(f.read(), startup, 'exec'), local)
            del os.environ['PYTHONSTARTUP']
        return local

    def start_shell(self, name):
        if not sys.stdin.isatty():
            available_shells = ['plain']
        else:
            available_shells = [name] if name else Shell.SHELLS

        for name in available_shells:
            try:
                return getattr(self, name)()
            except ImportError:  # pragma: no cover noqa: F401
                pass
        self.plain()  # pragma: no cover


def make_nameko_helper(config):
    """Create a fake module that provides some convenient access to nameko
    standalone functionality for interactive shell usage.
    """
    module = ModuleType('nameko')
    module.__doc__ = """Nameko shell helper for making rpc calls and dispatching
events.

Usage:
    >>> n.rpc.service.method()
    "reply"

    >>> n.dispatch_event('service', 'event_type', 'event_data')
"""
    from nameko.standalone.rpc import ClusterRpcProxy
    proxy = ClusterRpcProxy(config)
    module.rpc = proxy.start()
    from nameko.standalone.events import event_dispatcher
    module.dispatch_event = event_dispatcher(config)
    module.config = config
    module.disconnect = proxy.stop
    return module


def main(args):
    sys.path.append('.')  # Add current working directory to System path
    if args.config:
        with open(args.config) as fle:
            config = yaml.unsafe_load(fle)
        broker_from = " (from --config)"
    else:
        config = {AMQP_URI_CONFIG_KEY: args.broker}
        broker_from = ""

    banner = 'Nameko Python %s shell on %s\nBroker: %s%s' % (
        sys.version,
        sys.platform,
        config[AMQP_URI_CONFIG_KEY],
        broker_from
    )

    # ctx will be initialized via this lambda func after shell runner start
    def ctx_func():
        return {'n': make_nameko_helper(config)}

    runner = ShellRunner(banner, ctx_func)
    runner.start_shell(name=args.interface)
