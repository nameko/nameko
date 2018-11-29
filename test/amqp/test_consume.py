import eventlet
import pytest
from kombu.messaging import Queue
from mock import Mock, patch

from nameko.amqp.consume import Consumer
from nameko.amqp.publish import Publisher


class TestConsumer(object):

    @pytest.fixture
    def queue(self):
        return Queue(name="queue")

    @pytest.fixture
    def publisher(self, amqp_uri):
        return Publisher(amqp_uri)

    def test_consume_from_queue(self, queue, publisher, amqp_uri):

        tracker = Mock()

        def callback(body, message):
            tracker(body)
            message.ack()

        publisher.publish("payload", routing_key=queue.name, declare=[queue])

        consumer = Consumer(
            amqp_uri, queues=[queue], callbacks=[callback]
        )
        next(consumer.consume())

        assert tracker.call_count == 1

    def test_wait_until_consumer_ready(self, queue, publisher, amqp_uri):

        consumer = Consumer(amqp_uri, queues=[queue])

        def waiter():
            consumer.wait_until_consumer_ready()

        gt = eventlet.spawn(waiter)

        assert not consumer.ready.is_set()
        assert not gt.dead

        publisher.publish("payload", routing_key=queue.name, declare=[queue])
        next(consumer.consume())

        assert consumer.ready.is_set()
        gt.wait()  # make sure gt gets scheduled and has chance to exit
        assert gt.dead

    def test_ack_message_ignores_connection_errors(
        self, queue, publisher, amqp_uri
    ):
        publisher.publish("payload", routing_key=queue.name, declare=[queue])

        messages = []

        consumer = Consumer(
            amqp_uri,
            queues=[queue],
            callbacks=[lambda body, message: messages.append(message)]
        )
        next(consumer.consume())

        consumer.connection.close()

        for message in messages:
            with patch.object(message, 'ack') as ack:
                consumer.ack_message(message)
            assert not ack.called

    def test_requeue_message_ignores_connection_errors(
        self, queue, publisher, amqp_uri
    ):
        publisher.publish("payload", routing_key=queue.name, declare=[queue])

        messages = []

        consumer = Consumer(
            amqp_uri,
            queues=[queue],
            callbacks=[lambda body, message: messages.append(message)]
        )
        next(consumer.consume())

        consumer.connection.close()

        for message in messages:
            with patch.object(message, 'requeue') as requeue:
                consumer.requeue_message(message)
            assert not requeue.called

    def test_messages_requeued_after_stop(self, amqp_uri):

        messages = []

        consumer = Consumer(
            amqp_uri,
            callbacks=[lambda body, message: messages.append(message)]
        )

        message = Mock()

        consumer.on_message("body", message)

        assert len(messages) == 1
        assert not message.requeue.called

        consumer.stop()

        consumer.on_message("body", message)

        assert len(messages) == 1  # still 1, callback not called
        assert message.requeue.called
