
import functools

from nameko.legacy.channelhandler import ChannelHandler


def autoretry(func):
    """A decorator for connection.autoretry. """
    @functools.wraps(func)
    def wrapped(connection, *args, **kwargs):
        channel = kwargs.pop('channel', None)
        return connection.autoretry(func, channel=channel)(*args, **kwargs)
    return wrapped


def ensure(func):
    """wraps an entire function with ensure to use as a decorator. """
    @functools.wraps(func)
    def wrapped(connection, *args, **kwargs):
        with ChannelHandler(connection, create_channel=False) as ch:
            #return connection.ensure(ch, func, errback=None)(connection,
            #    *args, **kwargs)
            return ch((ch, func), connection, *args, **kwargs)
    return wrapped
