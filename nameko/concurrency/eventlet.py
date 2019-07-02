from eventlet import (  # noqa: F401
    Timeout, backdoor, getcurrent, listen, monkey_patch, sleep, spawn,
    spawn_after, spawn_n
)
from eventlet.event import Event as EventletEvent
from eventlet.greenpool import GreenPool  # noqa: F401
from eventlet.queue import Queue  # noqa: F401
from eventlet.semaphore import Semaphore  # noqa: F401


class Event(EventletEvent):
    """Eventlet event with gevet methods added."""

    def set(self):
        return self.send()


def wait(gt):
    return gt.wait()


def setup_backdoor(runner, backdoor_port):
    def _bad_call():
        raise RuntimeError(
            "This would kill your service, not close the backdoor. To exit, "
            "use ctrl-c."
        )

    host, port = backdoor_port

    socket = listen((host, port))
    # work around https://github.com/celery/kombu/issues/838
    socket.settimeout(None)
    gt = spawn(
        backdoor.backdoor_server,
        socket,
        locals={"runner": runner, "quit": _bad_call, "exit": _bad_call},
    )
    return socket, gt
