import socket

from eventlet import (  # noqa: F401
    Timeout, backdoor, getcurrent, listen, monkey_patch, sleep, spawn,
    spawn_after, spawn_n, wsgi
)
from eventlet.event import Event as EventletEvent
from eventlet.greenpool import GreenPool  # noqa: F401
from eventlet.queue import Queue  # noqa: F401
from eventlet.semaphore import Semaphore  # noqa: F401
from eventlet.support import get_errno


try:
    STATE_IDLE = wsgi.STATE_IDLE
except AttributeError:  # pragma: no cover
    STATE_IDLE = None


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


def get_wsgi_server(sock, wsgi_app, protocol, debug=False, log=None):
    return wsgi.Server(
        sock,
        sock.getsockname(),
        wsgi_app,
        protocol=protocol,
        debug=debug,
        log=log
    )


def process_wsgi_request(server, sock, address):
    try:
        if STATE_IDLE:  # pragma: no cover
            # eventlet >= 0.22
            # see https://github.com/eventlet/eventlet/issues/420
            server.process_request([address, sock, STATE_IDLE])
        else:  # pragma: no cover
            server.process_request((sock, address))

    except OSError as exc:
        # OSError("raw readinto() returned invalid length")
        # can be raised when a client disconnects very early as a result
        # of an eventlet bug: https://github.com/eventlet/eventlet/pull/353
        # See https://github.com/onefinestay/nameko/issues/368
        if "raw readinto() returned invalid length" in str(exc):
            return
        raise


class HttpOnlyProtocol(wsgi.HttpProtocol):
    # identical to HttpProtocol.finish, except greenio.shutdown_safe
    # is removed. it's only needed to ssl sockets which we don't support
    # this is a workaround until
    # https://bitbucket.org/eventlet/eventlet/pull-request/42
    # or something like it lands
    def finish(self):
        try:
            # patched in depending on python version; confuses pylint
            # pylint: disable=E1101
            wsgi.BaseHTTPServer.BaseHTTPRequestHandler.finish(self)
        except socket.error as e:
            # Broken pipe, connection reset by peer
            if get_errno(e) not in wsgi.BROKEN_SOCK:
                raise
        self.connection.close()
