from nameko.exceptions import RemoteError


def ifirst(iter_, ack=False):
    for i in iter_:
        if ack:
            i.ack()
        return i


def first(iter_, ack_all=False, ack_others=True):
    ret = ifirst(iter_, ack=ack_all)
    for i in iter_:
        if ack_others:
            i.ack()
    return ret


def last(iter_, ack_all=False, ack_others=True):
    i = None
    prev = None
    for i in iter_:
        if ack_others and prev is not None:
            prev.ack()
        prev = i
    if ack_all:
        i.ack()
    return i


def iter_rpcresponses(iter_):
    for msg in iter_:
        data = msg.payload
        if data['failure']:
            msg.ack()
            raise RemoteError(*data['failure'])
        elif data.get('ending', False):
            msg.ack()
            return
        else:
            yield msg
