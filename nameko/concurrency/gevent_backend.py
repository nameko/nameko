from gevent import spawn_later as spawn_after  # noqa: F401
from gevent import Timeout, getcurrent, sleep, spawn  # noqa: F401
from gevent.backdoor import BackdoorServer
from gevent.event import Event as GeventEvent  # noqa: F401
from gevent.event import AsyncResult
from gevent.greenlet import Greenlet  # noqa: F401
from gevent.lock import Semaphore  # noqa: F401
from gevent.monkey import patch_all as monkey_patch  # noqa: F401
from gevent.pool import Pool as GeventPool
from gevent.pywsgi import WSGIServer
from gevent.queue import Queue  # noqa: F401
from gevent.server import AF_INET, _tcp_listener
from greenlet import GreenletExit  # pylint: disable=E0611


spawn_n = spawn


class Event(object):
    __slots__ = ('async_result',)

    def __init__(self):
        self.async_result = AsyncResult()

    def send(self, result=None, exc=None):
        if exc is not None:
            if isinstance(exc, BaseException):
                return self.async_result.set_exception(exc)
            return self.async_result.set_exception(None, exc_info=exc)
        return self.async_result.set_result(result)

    def send_exception(self, *args):
        if len(args) == 1:
            return self.async_result.set_exception(args[0])
        else:
            return self.async_result.set_exception(None, exc_info=args)

    def reset(self):
        self.async_result = AsyncResult()

    def wait(self, timeout=None):
        result = self.async_result.get(timeout=timeout)
        return result

    def ready(self):
        return self.async_result.ready()


class Pool(GeventPool):
    def waitall(self):
        return self.join()

    def free(self):
        return self.free_count()


def resize_queue(queue, new_size):
    queue.maxsize += 1


def wait(gt):
    result = gt.get()

    if isinstance(result, GreenletExit):
        raise result
    return result


def get_waiter_count(semaphore):
    """Return the number of greenthreads linked to the lock."""
    return semaphore.linkcount()


def listen(addr, family=AF_INET, backlog=50, reuse_addr=True):
    """Copied signature from eventlet.listener (reuse_port left out)."""
    return _tcp_listener(addr, family=family, reuse_addr=reuse_addr)


def setup_backdoor(runner, backdoor_port):
    def _bad_call():
        raise RuntimeError(
            "This would kill your service, not close the backdoor. To exit, "
            "use ctrl-c."
        )

    (host, port) = backdoor_port
    server = BackdoorServer(
        (host, port),
        locals={"runner": runner, "quit": _bad_call, "exit": _bad_call}
    )
    server.start()
    gt = spawn(server.serve_forever)
    # TODO: Clean up this ugly hack!
    #
    # Eventlet uses "fd" as an attribute name, so we just map our _sock object
    # onto that name to get (very narrow) compatability.
    server.socket.fd = server.socket._sock
    return server.socket, gt


def get_wsgi_server(sock, wsgi_app, protocol=None, debug=False, log=None):
    """Get the wsgi server.

    Note: Gevent wsgi server does a lot less with the response than eventlet.
    Eventlet has the :meth:`eventlet.wsgi.Server.handle_one_request` which does
    a lot of processing whereas gevent does nothing.
    """
    if log is None:
        log = 'default'
    server = WSGIServer(sock, wsgi_app, log=log)
    server.socket_timeout = None  # compatability with eventlet
    return server


def process_wsgi_request(server, sock, address):
    server.handle(sock, address)


def yield_thread():
    """Yield current running green thread,
    allowing other green threads to run.
    """
    sleep()


HttpOnlyProtocol = None  # compatability with evetlet, not needed in gevent
