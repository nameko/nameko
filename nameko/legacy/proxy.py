import kombu

from nameko.legacy.context import Context
from nameko.legacy import nova


def get_anon_context():
    return Context(user_id=None, project_id=1)


class RPCProxy(object):
    """ Supports calling other services using the format::

            >>> proxy = RPCProxy()
            >>> proxy.service_name.controller_name(arg1='foo', arg2='bar')
    """

    def __init__(self, uri='memory://', timeout=None, info=None,
                 context_factory=None, control_exchange=None):
        self.uri = uri
        self.timeout = timeout
        self.info = info or []
        self.context_factory = context_factory or get_anon_context
        self.control_exchange = control_exchange

    def create_connection(self):
        return kombu.BrokerConnection(
            self.uri,
            transport_options={'confirm_publish': True},
        )

    def call_options(self):
        options = {}
        if self.control_exchange:
            options['CONTROL_EXCHANGE'] = self.control_exchange
        return options

    def __getattr__(self, key):
        if len(self.info) >= 2:
            raise AttributeError(key)
        info = self.info[:] + [key]
        return self.__class__(
            uri=self.uri,
            timeout=self.timeout,
            info=info,
            context_factory=self.context_factory,
            control_exchange=self.control_exchange,
        )

    def _get_route(self, kwargs):
        info = self.info

        if len(info) == 2:
            topic, method = info

        elif len(info) < 2:
            if len(info) == 1:
                topic = info[0]
            else:
                topic = kwargs.pop('topic', None)
            method = kwargs.pop('method', None)

            if None in (topic, method):
                raise ValueError('topic and method need providing')

        elif len(info) > 2:
            raise ValueError(
                'Only topic and method should be specified. '
                'Got topic={}, method={}, extra={}'.format(
                    info[0], info[1], ', '.join(info[2:]))
            )

        return (topic, method)

    def call(self, context=None, **kwargs):
        topic, method = self._get_route(kwargs)
        timeout = kwargs.pop('timeout', self.timeout)

        if context is None:
            context = self.context_factory()

        with self.create_connection() as connection:
            return nova.call(
                connection, context, topic,
                {'method': method, 'args': kwargs, },
                timeout=timeout,
                options=self.call_options(),
            )

    def __call__(self, context=None, **kwargs):
        return self.call(context, **kwargs)
