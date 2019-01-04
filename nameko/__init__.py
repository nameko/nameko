from copy import deepcopy


config = {}
"""
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


class setup_config:
    """
    A context manager for setting up a complete config for the given context

    Intended to be used mainly in tests::

        @pytest.yield_fixture
        def config():
            with setup_config({"spam": "ham"}):
                yield

    """

    def __init__(self, context_config):
        self.original_data = deepcopy(config)
        config.clear()
        config.update(context_config)

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        config.clear()
        config.update(self.original_data)


class update_config(setup_config):
    """
    A context manager for updating config just for the given context

    Intended to be used mainly in tests:

        @pytest.yield_fixture
        def memory_rabbit_config():
            with update_config({"AMQP_URI": "memory://"}):
                yield
    """

    def __init__(self, context_config):
        self.original_data = deepcopy(config)
        config.update(context_config)
