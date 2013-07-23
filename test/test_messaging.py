from kombu import Exchange, Queue
from mock import patch, Mock

from nameko.messaging import Publisher, ConsumeProvider

foobar_ex = Exchange('foobar_ex', durable=False)
foobar_queue = Queue('foobar_queue', exchange=foobar_ex, durable=False)


def test_consume_provider():

    consume_provider = ConsumeProvider(queue=foobar_queue,
                                       requeue_on_error=False)
    queue_consumer = Mock()
    message = Mock()

    with patch('nameko.messaging.get_queue_consumer') as get_queue_consumer:
        get_queue_consumer.return_value = queue_consumer

        # test lifecycle
        consume_provider.start()
        queue_consumer.add_consumer.assert_called_once_with(
            foobar_queue, consume_provider.handle_message)

        consume_provider.on_container_started()
        queue_consumer.start.assert_called_once_with()

        consume_provider.stop()
        queue_consumer.stop.assert_called_once_with()

        def successful_call(method, args, kwargs, callback):
            callback("result", None)

        def failed_call(method, args, kwargs, callback):
            callback(None, Exception("error"))

        # test handling successful call
        queue_consumer.reset_mock()
        with patch.object(consume_provider, "container") as container:
            container.spawn_worker.side_effect = successful_call

            consume_provider.handle_message("body", message)
            queue_consumer.ack_message.assert_called_once_with(message)

        # test handling failed call...
        with patch.object(consume_provider, "container") as container:
            container.spawn_worker.side_effect = failed_call

            # without requeue
            queue_consumer.reset_mock()
            consume_provider.handle_message("body", message)
            queue_consumer.ack_message.assert_called_once_with(message)

            # with requeue
            queue_consumer.reset_mock()
            consume_provider.requeue_on_error = True

            consume_provider.handle_message("body", message)
            assert not queue_consumer.ack_message.called
            queue_consumer.requeue_message.assert_called_once_with(message)


def test_publish_exchange_only():
    producer = Mock()
    publisher = Publisher(exchange=foobar_ex)

    with patch.object(publisher, '_connection') as connection, \
        patch('nameko.messaging.producers') as producers_pool, \
        patch('nameko.messaging.maybe_declare') as maybe_declare:

        producers_pool[connection].acquire().__enter__.return_value = producer

        msg = "msg"
        publisher.__call__(msg)

        maybe_declare.assert_called_once_with(foobar_ex, producer.channel)
        producer.publish.assert_called_once_with(msg, exchange=foobar_ex)


def test_publish_to_specific_queue():
    producer = Mock()
    publisher = Publisher(queue=foobar_queue)

    with patch.object(publisher, '_connection') as connection, \
        patch('nameko.messaging.producers') as producers_pool, \
        patch('nameko.messaging.maybe_declare') as maybe_declare:

        producers_pool[connection].acquire().__enter__.return_value = producer

        msg = "msg"
        publisher.__call__(msg)

        maybe_declare.assert_called_once_with(foobar_queue, producer.channel)
        producer.publish.assert_called_once_with(msg, exchange=foobar_ex)



