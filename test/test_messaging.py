import eventlet
from kombu import Exchange, Queue

from nameko.messaging import consume, Publisher


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


def test_publish_to_exchange_without_queue(start_service):
    spammer = start_service(ExchangeSpammer, 'exchangespammer')
    msg = 'exchange message'
    spammer._publish(msg)

    srv = start_service(Foobar, 'foobar')

    spammer._publish(msg)

    messages = srv.messages
    with eventlet.timeout.Timeout(CONSUME_TIMEOUT):
        while not messages:
            eventlet.sleep()

    assert messages == [msg]


def test_simple_publish_consume(start_service):
    spammer = start_service(QueueSpammer, 'queuespammer')
    msg = 'simple message'
    spammer._publish(msg)

    srv = start_service(Foobar, 'foobar')

    messages = srv.messages
    with eventlet.timeout.Timeout(CONSUME_TIMEOUT):
        while not messages:
            eventlet.sleep()

    assert messages == [msg]


def test_simple_consume_QOS_prefetch_1(start_service):
    srv1 = start_service(Foobar, 'foobar')
    srv2 = start_service(Foobar, 'foobar')

    spammer = start_service(QueueSpammer, 'queuespammer')

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


def test_consumer_fails_no_requeues(start_service):
    srv = start_service(BrokenFoobar, 'foobar')

    spammer = start_service(QueueSpammer, 'queuespammer')
    msg = 'fail_message'
    spammer._publish(msg)

    messages = srv.messages
    with eventlet.timeout.Timeout(CONSUME_TIMEOUT):
        while len(messages) < 1:
            eventlet.sleep()

    eventlet.sleep()
    assert messages == [msg]


def test_consumer_failure_requeues(start_service):
    srv = start_service(RequeueingFoobar, 'foobar')

    spammer = start_service(QueueSpammer, 'foobar')
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
    srv = start_service(Foobar, 'foobar')

    messages = srv.messages
    with eventlet.timeout.Timeout(CONSUME_TIMEOUT):
        while not messages:
            eventlet.sleep()

    assert messages == [msg]
