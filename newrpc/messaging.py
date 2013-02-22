from functools import partial
from logging import getLogger
from weakref import WeakKeyDictionary
import inspect

from kombu.common import maybe_declare
from kombu.pools import producers


log = getLogger(__name__)

consumer_configs = WeakKeyDictionary()


class ConsumerConfig(object):
    def __init__(self, queue, include_raw_message):
        self.queue = queue
        self.include_raw_message = include_raw_message
        self.prefetch_count = 1


class Publisher(object):
    def __init__(self, exchange=None, queue=None):
        self.exchange = exchange
        self.queue = queue

    def get_instance(self, connection):
        exchange = self.exchange
        queue = self.queue

        if exchange is None and queue is not None:
            exchange = queue.exchange

        def do_publish(msg):
            with producers[connection].acquire(block=True) as producer:
                channel = producer.channel
                if queue is not None:
                    maybe_declare(queue, channel)

                elif exchange is not None:
                    maybe_declare(exchange, channel)

                producer.publish(msg, exchange=exchange)

        return do_publish


def consume(queue, fn=None):
    if fn is None:
        return partial(consume, queue)

    argspecs = inspect.getargspec(fn)
    include_raw_message = len(argspecs.args) > 2
    consumer_configs[fn] = ConsumerConfig(queue, include_raw_message)

    return fn


def get_consumers(Consumer, service, on_message):

    for name, consumer_method in inspect.getmembers(service, inspect.ismethod):
        try:
            consumer_config = consumer_configs[consumer_method.im_func]

            consumer = Consumer(
                            queues=[consumer_config.queue],
                            callbacks=[partial(on_message, consumer_config, consumer_method)]
                            )

            consumer.qos(prefetch_count=consumer_config.prefetch_count)

            yield consumer
        except KeyError:
            pass


def process_message(consumer_config, consumer, body, message):
    try:
        if consumer_config.include_raw_message:
            consumer(body, message)
        else:
            consumer(body)

    except Exception as e:
        if message.acknowledged:
            log.error('failed to consume message, '
                    'cannot requeue because message already acknowledged: %s(): %s',
                    consumer, e)
        else:
            log.error('failed to consume message, requeueing message: %s(): %s',
                    consumer, e)
            message.requeue()
    else:
        if not message.acknowledged:
            message.ack()




