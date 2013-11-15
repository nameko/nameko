from nameko.exceptions import RemoteError


def ifirst(iter_):
    # TODO: this is rather confusing as it would return the same
    #       for an empty iterable and one with the first item being None.
    #       How would we know if it actually was the first item?
    for i in iter_:
        return i


def last(iter_):
    i = None
    prev = None
    for i in iter_:
        if prev is not None:
            prev.ack()
        prev = i
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
    # TODO: This might work even if we never have a data['ending'] == True
    #       and the sender closes it's connection prematurely,
    #       using memory transport. It should not! Needs further investigation.
