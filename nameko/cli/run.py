from __future__ import print_function

import eventlet
eventlet.monkey_patch()  # noqa (code before rest of imports)

import errno
import logging
import logging.config
import signal
import sys

from eventlet import backdoor

from nameko import config
from nameko.cli.utils import import_services
from nameko.runners import ServiceRunner


logger = logging.getLogger(__name__)


def setup_backdoor(runner, backdoor_port):
    def _bad_call():
        raise RuntimeError(
            "This would kill your service, not close the backdoor. To exit, "
            "use ctrl-c."
        )

    host, port = backdoor_port
    socket = eventlet.listen((host, port))
    # work around https://github.com/celery/kombu/issues/838
    socket.settimeout(None)
    gt = eventlet.spawn(
        backdoor.backdoor_server,
        socket,
        locals={"runner": runner, "quit": _bad_call, "exit": _bad_call},
    )
    return socket, gt


def run(service_modules, backdoor_port=None):

    if "." not in sys.path:
        sys.path.append(".")

    services = []
    for path in service_modules:
        services.extend(import_services(path))

    service_runner = ServiceRunner()
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


def main(services, backdoor_port):
    # sys.path already manipulated at services import time
    if "LOGGING" in config:
        logging.config.dictConfig(config["LOGGING"])
    else:
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    run(services, backdoor_port=backdoor_port)
