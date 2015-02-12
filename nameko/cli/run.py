"""Run a nameko service.

Given a python path to a module containing a nameko service, will host and run
it. By default this will try to find "service-looking" classes, but this can be
provided via ``nameko run module:ServiceClass``.
"""

import eventlet
eventlet.monkey_patch()  # noqa (code before rest of imports)

import errno
import inspect
import logging
import os
import signal
import sys

from eventlet import backdoor

from nameko.constants import AMQP_URI_CONFIG_KEY
from nameko.exceptions import CommandError
from nameko.extensions import ENTRYPOINT_EXTENSIONS_ATTR
from nameko.runners import ServiceRunner


logger = logging.getLogger(__name__)


def is_type(obj):
    return isinstance(obj, type)


def is_entrypoint(method):
    return hasattr(method, ENTRYPOINT_EXTENSIONS_ATTR)


def import_service(module_name):
    parts = module_name.split(":", 1)
    if len(parts) == 1:
        module_name, obj = module_name, None
    else:
        module_name, obj = parts[0], parts[1]

    try:
        __import__(module_name)
    except ImportError as exc:
        if module_name.endswith(".py") and os.path.exists(module_name):
            raise CommandError(
                "Failed to find service, did you mean '{}'?".format(
                    module_name[:-3].replace('/', '.')
                )
            )

        missing_module_message = 'No module named {}'.format(module_name)
        # is there a better way to do this?
        if exc.message != missing_module_message:
            # found module, but importing it raised an import error elsewhere
            # let this bubble (resulting in a full stacktrace being printed)
            raise

        raise CommandError(exc)

    module = sys.modules[module_name]

    if obj is None:
        found_services = []
        # find top-level objects with entrypoints
        for _, potential_service in inspect.getmembers(module, is_type):
            if inspect.getmembers(potential_service, is_entrypoint):
                found_services.append(potential_service)

        if not found_services:
            raise CommandError(
                "Failed to find anything that looks like a service in module "
                "{!r}".format(module_name)
            )

        elif len(found_services) > 1:
            found_service_names = ', '.join(
                service.__name__ for service in found_services
            )
            raise CommandError(
                "Found multiple potential services in module {!r}: "
                "{!s}".format(module_name, found_service_names)
            )
        else:
            service_cls = found_services[0]

    else:

        try:
            service_cls = getattr(module, obj)
        except AttributeError:
            raise CommandError(
                "Failed to find service class {!r} in module {!r}".format(
                    obj, module_name)
            )

    if not isinstance(service_cls, type):
        raise CommandError("Service must be a class.")
    return service_cls


def setup_backdoor(runner, port):
    def _bad_call():
        raise RuntimeError(
            'This would kill your service, not close the backdoor. To exit, '
            'use ctrl-c.')
    socket = eventlet.listen(('localhost', port))
    gt = eventlet.spawn(
        backdoor.backdoor_server,
        socket,
        locals={
            'runner': runner,
            'quit': _bad_call,
            'exit': _bad_call,
        })
    return socket, gt


def run(services, config, backdoor_port=None):
    service_runner = ServiceRunner(config)
    for service_cls in services:
        service_runner.add_service(service_cls)

    def shutdown(signum, frame):
        # signal handlers are run by the MAINLOOP and cannot use eventlet
        # primitives, so we have to call `stop` in a greenlet
        eventlet.spawn_n(service_runner.stop)

    signal.signal(signal.SIGTERM, shutdown)

    if backdoor_port is not None:
        setup_backdoor(service_runner, backdoor_port)

    service_runner.start()

    # if the signal handler fires while eventlet is waiting on a socket,
    # the __main__ greenlet gets an OSError(4) "Interrupted system call".
    # This is a side-effect of the eventlet hub mechanism. To protect nameko
    # from seeing the exception, we wrap the runner.wait call in a greenlet
    # spawned here, so that we can catch (and silence) the exception.
    runnlet = eventlet.spawn(service_runner.wait)

    while True:
        try:
            runnlet.wait()
        except OSError as exc:
            if exc.errno == errno.EINTR:
                # this is the OSError(4) caused by the signalhandler.
                # ignore and go back to waiting on the runner
                continue
            raise
        except KeyboardInterrupt:
            try:
                service_runner.stop()
            except KeyboardInterrupt:
                service_runner.kill()
        else:
            # runner.wait completed
            break


def main(args):
    logging.basicConfig(level=logging.INFO)

    if '.' not in sys.path:
        sys.path.insert(0, '.')

    cls = import_service(args.service)

    config = {AMQP_URI_CONFIG_KEY: args.broker}
    run([cls], config, backdoor_port=args.backdoor_port)


def init_parser(parser):
    parser.add_argument(
        'service', metavar='module[:service class]',
        help='python path to the service class to run')
    parser.add_argument(
        '--broker', default='amqp://guest:guest@localhost:5672/nameko',
        help='RabbitMQ broker url')
    parser.add_argument(
        '--backdoor-port', type=int,
        help='Specity a port number to host a backdoor, which can be connected'
        ' to for an interactive interpreter within the running service'
        ' process using `nameko backdoor`.'
    )
    return parser
