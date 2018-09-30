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
