import types


class ChannelHandler(object):
    def __init__(self, connection, channel=None, create_channel=True):
        self.connection = connection
        if channel is None and create_channel:
            self.channel = connection.channel()
        else:
            self.channel = channel

    def close(self):
        if self.channel is not None:
            self.channel.close()

    def revive(self, channel):
        self.channel = channel

    def on_error(self, exc_value, *args, **kwargs):
        # TODO: looking at ensure() it sounds like
        # we are ignoring any error, is this intentional?
        pass

    def __call__(self, func, *args, **kwargs):
        return self.ensure(func)(*args, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_typ, exc_val, exc_tb):
        self.close()

    def ensure(self, func):
        if isinstance(func, types.MethodType):
            obj = func.im_self
        elif isinstance(func, tuple):
            obj, func = func
        else:
            raise TypeError('could not obtain object from function!')

        if obj is self:
            revive = self.revive
        else:
            revive = None

        return self.connection.ensure(
            obj, func,
            errback=self.on_error,
            on_revive=revive)
