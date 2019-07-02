import six
from gevent import spawn as spawn_n
from gevent import spawn_later as spawn_after
from gevent import *
from gevent import Timeout, getcurrent, sleep
from gevent._util import _NONE
from gevent.backdoor import BackdoorServer
from gevent.event import Event as GeventEvent
from gevent.event import AsyncResult
from gevent.greenlet import Greenlet
from gevent.lock import Semaphore
from gevent.monkey import patch_all as monkey_patch
from gevent.pool import Pool as GeventPool
from gevent.queue import Queue
from gevent.server import AF_INET, _tcp_listener
from greenlet import GreenletExit


class Event():
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

# class Event(GeventEvent):
#     """Gevent event with eventlet methods added."""

#     def __init__(self, *args, **kwargs):
#         super(Event, self).__init__(*args, **kwargs)
#         self._exc = None
#         self._result = None

#     def send(self, result=None, exc=None):
#         if exc is not None and not isinstance(exc, tuple):
#             self._exc = (exc,)
#         else:
#             self._exc = exc
#         self._result = result
#         self.set()

#     def send_exception(self, *args):
#         return self.send(exc=args)

#     def wait(self):
#         result = super(Event, self).wait()
#         if not result:
#             raise NotImplementedError('Timeout not supported.')
#         if self._exc is not None:
#             if len(self._exc) == 1:
#                 raise self._exc[0]
#             six.reraise(*self._exc)
#         else:
#             return self._result

#     def reset(self):
#         self._exc = None
#         self._result = None
#         self.clear()


class GreenPool(GeventPool):
    def waitall(self):
        return self.join()

    def link(self, callback, *args, **kwargs):
        if args or kwargs:
            def f(gt):
                return callback(gt, *args, **kwargs)
        else:
            f = callback
        return super(GreenPool, self).link(f)


def wait(gt):
    result = gt.get()

    if isinstance(result, GreenletExit):
        raise result


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
    server = BackdoorServer((host, port), locals={"runner": runner, "quit": _bad_call, "exit": _bad_call})
    server.start()
    gt = spawn(server.serve_forever)
    # TODO: Clean up this ugly hack!
    #
    # Eventlet uses "fd" as an attribute name, so we just map our _sock object
    # onto that name to get (very narrow) compatability.
    server.socket.fd = server.socket._sock
    return server.socket, gt
