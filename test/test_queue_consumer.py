from nameko.messaging import QueueConsumer
from kombu import Queue, Exchange

import eventlet
from eventlet.event import Event

TIMEOUT = 5


def test_lifecycle(reset_rabbit, rabbit_manager, rabbit_config):

    amqp_uri = rabbit_config['amqp_uri']

    qconsumer = QueueConsumer(amqp_uri)

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

    qconsumer = QueueConsumer(amqp_uri)

    qconsumer.start()
    gt = qconsumer._gt

    # nothing should happen as the consumer has already been started
    qconsumer.start()
    assert gt is qconsumer._gt

    # we should be able to call stop multiple times without errors
    qconsumer.stop()
    qconsumer.stop()
