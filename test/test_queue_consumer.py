import eventlet
from eventlet.event import Event
from kombu import Queue, Exchange, Connection
from kombu.exceptions import TimeoutError
from mock import patch

from nameko.messaging import QueueConsumer, AMQP_URI_CONFIG_KEY


TIMEOUT = 5


def test_lifecycle(reset_rabbit, rabbit_manager, rabbit_config):

    amqp_uri = rabbit_config[AMQP_URI_CONFIG_KEY]

    qconsumer = QueueConsumer(amqp_uri, 3)

    exchange = Exchange('spam')
    queue = Queue('ham', exchange=exchange)

    on_message_called = Event()

    def on_message(body, message):
        on_message_called.send(message)

    qconsumer.add_consumer(queue, on_message)

    qconsumer.start()

    vhost = rabbit_config['vhost']
    rabbit_manager.publish(vhost, 'spam', '', 'shrub')

    message = on_message_called.wait()

    gt = eventlet.spawn(qconsumer.stop)
    eventlet.sleep(0)

    with eventlet.Timeout(TIMEOUT):
        while not qconsumer._consumers_stopped:
            eventlet.sleep()

    # stop has to wait for all messages to be acked before shutting down
    assert not gt.dead

    # the consumer should have stopped and not accept any new messages
    rabbit_manager.publish(vhost, 'spam', '', 'ni')

    # this should cause the consumer to finish shutting down
    qconsumer.ack_message(message)
    with eventlet.Timeout(TIMEOUT):
        gt.wait()

    # there should be a message left on the queue
    messages = rabbit_manager.get_messages(vhost, 'ham')
    assert ['ni'] == [msg['payload'] for msg in messages]


def test_reentrant_start_stops(reset_rabbit, rabbit_config):
    amqp_uri = rabbit_config[AMQP_URI_CONFIG_KEY]

    qconsumer = QueueConsumer(amqp_uri, 3)

    qconsumer.start()
    gt = qconsumer._gt

    # nothing should happen as the consumer has already been started
    qconsumer.start()
    assert gt is qconsumer._gt

    # we should be able to call stop multiple times without errors
    qconsumer.stop()
    qconsumer.stop()


def test_stop_while_starting():
    started = Event()

    class BrokenConnConsumer(QueueConsumer):
        def consume(self, *args, **kwargs):
            started.send(None)
            # kombu will retry again and again on broken connections
            # so we have to make sure the event is reset to allow consume
            # to be called again
            started.reset()
            return super(BrokenConnConsumer, self).consume(*args, **kwargs)

    qconsumer = BrokenConnConsumer(amqp_uri=None, prefetch_count=3)

    with eventlet.Timeout(TIMEOUT):
        with patch.object(Connection, 'connect') as connect:
            # patch connection to raise an error
            connect.side_effect = TimeoutError('test')
            # try to start the queue consumer
            gt = eventlet.spawn(qconsumer.start)
            # wait for the queue consumer to begin starting and
            # then immediately stop it
            started.wait()

    with eventlet.Timeout(TIMEOUT):
        qconsumer.stop()

    with eventlet.Timeout(TIMEOUT):
        # we expect the qconsumer.start thread to finish
        # almost immediately adn when it does the qconsumer thread
        # should be dead too
        while not gt.dead:
            eventlet.sleep()

        assert qconsumer._gt.dead


def test_prefetch_count(reset_rabbit, rabbit_manager, rabbit_config):
    amqp_uri = rabbit_config[AMQP_URI_CONFIG_KEY]

    qconsumer1 = QueueConsumer(amqp_uri, 1)
    qconsumer2 = QueueConsumer(amqp_uri, 1)

    exchange = Exchange('spam')
    queue = Queue('spam', exchange=exchange)

    consumer_continue = Event()

    def handler1(body, message):
        consumer_continue.wait()
        qconsumer1.ack_message(message)

    messages = []

    def handler2(body, message):
        messages.append(body)
        qconsumer2.ack_message(message)

    qconsumer1.add_consumer(queue, handler1)
    qconsumer2.add_consumer(queue, handler2)
    qconsumer1.start()
    qconsumer2.start()

    vhost = rabbit_config['vhost']
    # the first consumer only has a prefetch_count of 1 and will only
    # consume 1 message and wait in handler1()
    rabbit_manager.publish(vhost, 'spam', '', 'ham')
    # the next message will go to handler2() no matter of any prefetch_count
    rabbit_manager.publish(vhost, 'spam', '', 'eggs')
    # the third message is only going to handler2 because the first consumer
    # has a prefetch_count of 1 and thus is unable to deal with another message
    # until having ACKed the first one
    rabbit_manager.publish(vhost, 'spam', '', 'bacon')

    with eventlet.Timeout(TIMEOUT):
        while len(messages) < 2:
            eventlet.sleep()

    # allow the waiting consumer to ack its message
    consumer_continue.send(None)

    assert messages == ['eggs', 'bacon']

    qconsumer1.stop()
    qconsumer2.stop()
