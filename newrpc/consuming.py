import eventlet

__all__ = ['consumefrom', ]

_conndrainers = {}


def _has_waiters(greenthread):
    return bool(greenthread._exit_event._waiters)


def consumefrom(conn, killdrainer=False):
    id_ = id(conn)
    gt = _conndrainers.get(id_)
    if gt is None or gt.dead:
        gt = _conndrainers[id_] = eventlet.spawn(conn.drain_events)
    try:
        return gt.wait()
    finally:
        if killdrainer and not _has_waiters(gt) and not gt.dead:
            gt.kill()
