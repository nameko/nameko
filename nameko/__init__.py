from collections import UserDict
from copy import deepcopy
from six import wraps


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

        config = self.data

        class Patcher:

            def __call__(self, func):
                @wraps(func)
                def wrapper(*args, **kwargs):
                    self.original_config = deepcopy(config)
                    if self.clear:
                        config.clear()
                    config.update(self.context_config)
                    try:
                        result = func(*args, **kwargs)
                    finally:
                        config.clear()
                        config.update(self.original_config)
                    return result
                return wrapper

            def __init__(self, context_config, clear=False):
                self.clear = clear
                self.context_config = context_config

            def __enter__(self):
                self.original_config = deepcopy(config)
                if self.clear:
                    config.clear()
                config.update(self.context_config)

            def __exit__(self, exc_type, exc_value, traceback):
                config.clear()
                config.update(self.original_config)
                pass

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
