from nameko.exceptions import RemoteError


def ifirst(iter_, ack=False):
    # TODO ack is always False
    for i in iter_:
        if ack:
            i.ack()
        return i


def first(iter_, ack_all=False, ack_others=True):
    # TODO: there is no user of this function
    ret = ifirst(iter_, ack=ack_all)
    for i in iter_:
        if ack_others:
            i.ack()
    return ret


def last(iter_, ack_all=False, ack_others=True):
    # TODO: ack_others is always True
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
