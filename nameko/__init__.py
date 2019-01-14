from copy import deepcopy
from six import wraps
from six.moves import UserDict


class Config(UserDict):

    def setup(self, config):
        self.data.clear()
        self.data.update(config)

    def patch(self, context_config, clear=False):
        """
        A context manager and decorator for updating config just for the given context
        or decorated function execution.

        Intended to be used mainly in tests:

            @pytest.yield_fixture
            def memory_rabbit_config():
                with nameko.config.patch({"AMQP_URI": "memory://"}):
                    yield

            @nameko.config.patch({"AMQP_URI": "memory://"})
            def test_spam():
                assert nameko.config["AMQP_URI"] == "memory://"

        If `clear` is set to True then the config is completely replaced with the given
        `context_config` otherwise it's only updated.

        """

        data = self.data

        class Patcher:

            def __call__(self, func):
                @wraps(func)
                def wrapper(*args, **kwargs):
                    self.__enter__()
                    try:
                        result = func(*args, **kwargs)
                    finally:
                        self.__exit__(*tuple())
                    return result
                return wrapper

            def __init__(self, context_data, clear=False):
                self.clear = clear
                self.context_data = context_data

            def __enter__(self):
                self.original_data = deepcopy(data)
                if self.clear:
                    data.clear()
                data.update(self.context_data)

            def __exit__(self, *exc_info):
                data.clear()
                data.update(self.original_data)

        return Patcher(context_config, clear=clear)


config = Config()
"""
A dictionary holding and managing configuration

It is possible to set the config by using ``--config`` switch of Nameko
command line interface.

The config can be used straight on service definition level, e.g.::

    from nameko import config
    from nameko.messaging import Consumer, Queue

    class Service:

        @consume(
            queue=Queue(
                exchange=config.MY_EXCHANGE,
                routing_key=config.MY_ROUTING_KEY,
                name=config.MY_QUEUE_NAME
            ),
            prefetch_count=config.MY_CONSUMER_PREFETCH_COUNT
        )
        def consume(self, payload):
            pass

As a plain dictionary it is normally mutable at any point. After all,
we're all consenting adults here.)
"""
