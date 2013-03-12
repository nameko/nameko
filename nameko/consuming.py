from logging import getLogger

import eventlet

from nameko.exceptions import WaiterTimeout


log = getLogger(__name__)

_conndrainers = {}


def _has_waiters(greenthread):
    return bool(greenthread._exit_event._waiters)


def consumefrom(conn):
    id_ = id(conn)
    gt = _conndrainers.get(id_)
    if gt is None or gt.dead:
        gt = _conndrainers[id_] = eventlet.spawn(conn.drain_events)
    try:
        return gt.wait()
    finally:
        if not _has_waiters(gt):
            _conndrainers.pop(id_, None)


def queue_iterator(queue, no_ack=False, timeout=None):
    channel = queue.channel
    buf = []

    def callback(message):
        try:
            message = channel.message_to_python(message)
        except AttributeError as e:
            # TODO: We should never have a channel without a
            #       .message_to_python()
            #       No tests come here, but we want to be safe
            #       until we are 100% sure.
            #       This also swallows errors during message deserialization.
            #       So, which issue are we trying to ignore.
            log.warn('Hidden Error, calling channel.message_to_python(): %s', e)

        buf.append(message)

    tag = queue.consume(callback=callback, no_ack=no_ack)
    with eventlet.Timeout(timeout, exception=WaiterTimeout()):
        try:
            while True:
                if buf:
                    yield buf.pop(0)
                consumefrom(channel.connection.client)
        finally:
            queue.cancel(tag)
