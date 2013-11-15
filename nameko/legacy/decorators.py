
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


def try_wraps(func):
    """Marks a function as wrapping another one using `functools.wraps`, but
    fails unobtrusively when this isn't possible"""
    do_wrap = functools.wraps(func)

    def try_to_wrap(inner):
        try:
            return do_wrap(inner)
        except AttributeError:
            # Some objects don't have all the attributes needed in order to
            # be wrapped. Fail gracefully and don't wrap.
            return inner

    return try_to_wrap
