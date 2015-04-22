"""Launch an interactive python shell for working with remote nameko services.

This is a regular interactive interpreter, with a special module ``n`` added
to the built-in namespace, providing ``n.rpc`` and ``n.dispatch_event``.
"""
import code
import os
import sys
from types import ModuleType

from nameko.constants import AMQP_URI_CONFIG_KEY
from nameko.standalone.rpc import ClusterRpcProxy
from nameko.standalone.events import event_dispatcher


SHELLS = ['bpython', 'ipython']


def init_parser(parser):
    parser.add_argument(
        '--broker', default='amqp://guest:guest@localhost',
        help='RabbitMQ broker url')
    parser.add_argument(
        '--interface', choices=SHELLS,
        help='Specify an interactive interpreter interface.')
    parser.add_argument(
        '--plain', action='store_true',
        help='Use the regular Python interpreter.')
    return parser


def make_nameko_helper(config):
    """Create a fake module that provides some convenient access to nameko
    standalone functionality for interactive shell usage.
    """
    module = ModuleType('nameko')
    module.__doc__ = """Nameko shell helper for making rpc calls and dispaching
events.

Usage:
    >>> n.rpc.service.method()
    "reply"

    >>> n.dispatch_event('service', 'event_type', 'event_data')
"""
    proxy = ClusterRpcProxy(config)
    module.rpc = proxy.start()
    module.dispatch_event = event_dispatcher(config)
    module.config = config
    module.disconnect = proxy.stop
    return module


def bpython(banner, local):
    import bpython
    bpython.embed(banner=banner, locals_=local)


def ipython(banner, local):
    from IPython import embed
    embed(banner1=banner, user_ns=local)


def run_shell(shell=None, banner=None, local=None):
    available_shells = [shell] if shell else SHELLS

    for shell in available_shells:
        try:
            return globals()[shell](banner, local)
        except ImportError:
            pass
    raise ImportError


def main(args):
    banner = 'Nameko Python %s shell on %s\nBroker: %s' % (
        sys.version,
        sys.platform,
        args.broker.encode('utf-8'),
    )
    config = {AMQP_URI_CONFIG_KEY: args.broker}

    ctx = {}
    ctx['n'] = make_nameko_helper(config)

    try:
        if args.plain:
            raise ImportError

        run_shell(shell=args.interface, banner=banner, local=ctx)
    except ImportError:
        # Support the regular Python interpreter startup script if someone
        # is using it.
        startup = os.environ.get('PYTHONSTARTUP')
        if startup and os.path.isfile(startup):
            with open(startup, 'r') as f:
                eval(compile(f.read(), startup, 'exec'), ctx)

        code.interact(banner=banner, local=ctx)
