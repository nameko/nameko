"""Run nameko services.  Given a python path to a module containing one or more
nameko services, will host and run them. By default this will try to find
classes that look like services (anything with nameko entrypoints), but a
specific service can be specified via ``nameko run module:ServiceClass``.  """

from __future__ import print_function

import eventlet
eventlet.monkey_patch()  # noqa (code before rest of imports)

import errno
import inspect
import logging
import logging.config
import os
import re
import signal
import sys

import yaml
from eventlet import backdoor
from nameko.constants import AMQP_URI_CONFIG_KEY
from nameko.exceptions import CommandError
from nameko.extensions import ENTRYPOINT_EXTENSIONS_ATTR
from nameko.runners import ServiceRunner


logger = logging.getLogger(__name__)

MISSING_MODULE_TEMPLATE = "^No module named '?{}'?$"


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

        missing_module_re = MISSING_MODULE_TEMPLATE.format(module_name)
        # is there a better way to do this?

        if re.match(missing_module_re, str(exc)):
            raise CommandError(exc)

        # found module, but importing it raised an import error elsewhere
        # let this bubble (resulting in a full stacktrace being printed)
        raise

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

        found_services = [service_cls]

    return found_services


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
            print()  # looks nicer with the ^C e.g. bash prints in the terminal
            try:
                service_runner.stop()
            except KeyboardInterrupt:
                print()  # as above
                service_runner.kill()
        else:
            # runner.wait completed
            break


def main(args):
    if '.' not in sys.path:
        sys.path.insert(0, '.')

    if args.config:
        with open(args.config) as fle:
            config = yaml.load(fle)
    else:
        config = {
            AMQP_URI_CONFIG_KEY: args.broker
        }

    if "LOGGING" in config:
        logging.config.dictConfig(config['LOGGING'])
    else:
        logging.basicConfig(level=logging.INFO, format='%(message)s')

    services = []
    for path in args.services:
        services.extend(
            import_service(path)
        )

    run(services, config, backdoor_port=args.backdoor_port)


def init_parser(parser):
    parser.add_argument(
        'services', nargs='+',
        metavar='module[:service class]',
        help='python path to one or more service classes to run')

    parser.add_argument(
        '--config', default='',
        help='The YAML configuration file')

    parser.add_argument(
        '--broker', default='amqp://guest:guest@localhost',
        help='RabbitMQ broker url')

    parser.add_argument(
        '--backdoor-port', type=int,
        help='Specify a port number to host a backdoor, which can be connected'
        ' to for an interactive interpreter within the running service'
        ' process using `nameko backdoor`.')

    return parser
