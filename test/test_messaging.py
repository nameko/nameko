import eventlet

from kombu import Exchange, Queue
from kombu.common import maybe_declare

from nameko.messaging import consume, Publisher
from nameko.service import Service

import conftest

foobar_ex = Exchange('foobar_ex', durable=False)
foobar_queue = Queue('foobar_queue', exchange=foobar_ex, durable=False)


CONSUME_TIMEOUT = 5


class QueueSpammer(object):

    publish = Publisher(queue=foobar_queue)

    def _publish(self, msg):
        self.publish(msg)


class ExchangeSpammer(QueueSpammer):

    # we only publish to an exchange, which has no queue.
    publish = Publisher(exchange=foobar_ex)


class Foobar(object):
    def __init__(self):
        self.messages = []

    @consume(queue=foobar_queue)
    def _consume(self, msg):
        eventlet.sleep()
        self.messages.append(msg)


class BrokenFoobar(Foobar):
    @consume(queue=foobar_queue)
    def _consume(self, msg):
        eventlet.sleep()
        self.messages.append(msg)
        raise Exception(msg)


class RequeueingFoobar(Foobar):
    @consume(queue=foobar_queue, requeue_on_error=True)
    def _consume(self, msg):
        eventlet.sleep()
        self.messages.append('rejected:' + msg)
        raise Exception(msg)


services = []


def teardown_function(fn):
    # we really don't want any services running
    # because, they will consume messages from
    # current tests
    for s in services:
        try:
            s.kill()
        except:
            pass
    del services[:]

    # We want to make sure queues get removed.
    # The services should create them as needed.
    with conftest.get_connection() as conn:
        with conn.channel() as channel:
            queue = foobar_queue(channel)
            maybe_declare(foobar_queue, channel)
            queue.delete()
            exchange = foobar_ex(channel)
            exchange.delete()


def _start_service(cls, get_connection):
    srv = Service(cls, get_connection, 'foo', 'bar')
    services.append(srv)
    srv.start()
    srv.consume_ready.wait()
    return srv.service


def test_publish_to_exchange_without_queue(get_connection):
    spammer = _start_service(ExchangeSpammer, get_connection)
    msg = 'exchange message'
    spammer._publish(msg)

    srv = _start_service(Foobar, get_connection)

    spammer._publish(msg)

    messages = srv.messages
    with eventlet.timeout.Timeout(CONSUME_TIMEOUT):
        while not messages:
            eventlet.sleep()

    assert messages == [msg]


def test_simple_publish_consume(get_connection):
    spammer = _start_service(QueueSpammer, get_connection)
    msg = 'simple message'
    spammer._publish(msg)

    srv = _start_service(Foobar, get_connection)

    messages = srv.messages
    with eventlet.timeout.Timeout(CONSUME_TIMEOUT):
        while not messages:
            eventlet.sleep()

    assert messages == [msg]


def test_simple_consume_QOS_prefetch_1(get_connection):
    srv1 = _start_service(Foobar, get_connection)
    srv2 = _start_service(Foobar, get_connection)

    spammer = _start_service(QueueSpammer, get_connection)

    msg = 'simple message 2'
    spammer._publish(msg)
    spammer._publish(msg)

    messages1 = srv1.messages
    with eventlet.timeout.Timeout(CONSUME_TIMEOUT):
        while not messages1:
            eventlet.sleep()

    messages2 = srv2.messages
    with eventlet.timeout.Timeout(CONSUME_TIMEOUT):
        while not messages2 and len(messages1) < 2:
            eventlet.sleep()

    assert messages1 == [msg]
    assert messages2 == [msg]


def test_consumer_fails_no_requeues(get_connection):
    srv = _start_service(BrokenFoobar, get_connection)

    spammer = _start_service(QueueSpammer, get_connection)
    msg = 'fail_message'
    spammer._publish(msg)

    messages = srv.messages
    with eventlet.timeout.Timeout(CONSUME_TIMEOUT):
        while len(messages) < 1:
            eventlet.sleep()

    eventlet.sleep()
    assert messages == [msg]


def test_consumer_failure_requeues(get_connection):
    srv = _start_service(RequeueingFoobar, get_connection)

    spammer = _start_service(QueueSpammer, get_connection)
    msg = 'reject_message'
    # this message will be requeued until
    # a non-failing service picks it up
    spammer._publish(msg)

    #lets wait unitl at least one has been requeued
    messages = srv.messages
    with eventlet.timeout.Timeout(CONSUME_TIMEOUT):
        while not messages:
            eventlet.sleep()

    #create a service to accept the failed messages
    srv = _start_service(Foobar, get_connection)

    messages = srv.messages
    with eventlet.timeout.Timeout(CONSUME_TIMEOUT):
        while not messages:
            eventlet.sleep()

    assert messages == [msg]
