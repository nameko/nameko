import socket

import eventlet
from eventlet.event import Event
import pytest

from nameko.messaging import QueueConsumer
from kombu import Queue, Exchange, Connection


TIMEOUT = 5


def test_lifecycle(reset_rabbit, rabbit_manager, rabbit_config):

    amqp_uri = rabbit_config['amqp_uri']

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
    amqp_uri = rabbit_config['amqp_uri']

    qconsumer = QueueConsumer(amqp_uri, 3)

    qconsumer.start()
    gt = qconsumer._gt

    # nothing should happen as the consumer has already been started
    qconsumer.start()
    assert gt is qconsumer._gt

    # we should be able to call stop multiple times without errors
    qconsumer.stop()
    qconsumer.stop()


def test_prefetch_count(reset_rabbit, rabbit_manager, rabbit_config):
    amqp_uri = rabbit_config['amqp_uri']

    qconsumer1 = QueueConsumer(amqp_uri, 1)
    qconsumer2 = QueueConsumer(amqp_uri, 1)

    exchange = Exchange('spam')
    queue = Queue('spam', exchange=exchange)

    godot = Event()

    def handler1(body, message):
        godot.wait()
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

    godot.send('will come the next day')

    assert messages == ['eggs', 'bacon']

    qconsumer1.stop()
    qconsumer2.stop()


# TODO: this is a pointless test
# It should signal the dependency provider errors it cannot recover from
# which, in turn, should signal the container, which should handle the error
def test_socket_error():

    class FailConnection(Connection):
        # a connection that will give a socket.error when the service starts
        def drain_events(self, *args, **kwargs):
            raise socket.error

    qconsumer = QueueConsumer(None, 3)
    qconsumer._connection = FailConnection(transport='memory')

    with pytest.raises(socket.error):
        qconsumer.start()
        # This is pointless, why would anyone want to wait for it?
        qconsumer._gt.wait()
