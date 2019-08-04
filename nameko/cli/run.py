from __future__ import print_function

from nameko.concurrency import monkey_patch_if_enforced

monkey_patch_if_enforced()  # noqa (code before rest of imports)

import errno
import logging
import logging.config
import signal

from nameko.concurrency import setup_backdoor, wait, spawn, spawn_n
from nameko import config
from nameko.runners import ServiceRunner


logger = logging.getLogger(__name__)


def run(services, backdoor_port=None):
    service_runner = ServiceRunner()
    for service_cls in services:
        service_runner.add_service(service_cls)

    def shutdown(signum, frame):
        # signal handlers are run by the MAINLOOP and cannot use eventlet
        # primitives, so we have to call `stop` in a greenlet
        spawn_n(service_runner.stop)

    signal.signal(signal.SIGTERM, shutdown)

    if backdoor_port is not None:
        setup_backdoor(service_runner, backdoor_port)

    service_runner.start()

    # if the signal handler fires while eventlet is waiting on a socket,
    # the __main__ greenlet gets an OSError(4) "Interrupted system call".
    # This is a side-effect of the eventlet hub mechanism. To protect nameko
    # from seeing the exception, we wrap the runner.wait call in a greenlet
    # spawned here, so that we can catch (and silence) the exception.
    runnlet = spawn(service_runner.wait)

    while True:
        try:
            wait(runnlet)
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
