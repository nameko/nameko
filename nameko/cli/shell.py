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


def init_parser(parser):
    parser.add_argument(
        '--broker', default='amqp://guest:guest@localhost:5672/nameko',
        help='RabbitMQ broker url')
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


def main(args):
    banner = 'Nameko Python %s shell on %s\nBroker: %s' % (
        sys.version,
        sys.platform,
        args.broker.encode('utf-8'),
    )
    config = {AMQP_URI_CONFIG_KEY: args.broker}

    ctx = {}
    ctx['n'] = make_nameko_helper(config)

    # Support the regular Python interpreter startup script if someone
    # is using it.
    startup = os.environ.get('PYTHONSTARTUP')
    if startup and os.path.isfile(startup):
        with open(startup, 'r') as f:
            eval(compile(f.read(), startup, 'exec'), ctx)

    code.interact(banner=banner, local=ctx)
