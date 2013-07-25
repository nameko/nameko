from kombu import Exchange, Queue
from mock import patch, Mock, ANY

from nameko.messaging import Publisher, ConsumeProvider
from nameko.testing.utils import (
    wait_for_call, as_context_manager, as_mock_property)

foobar_ex = Exchange('foobar_ex', durable=False)
foobar_queue = Queue('foobar_queue', exchange=foobar_ex, durable=False)

CONSUME_TIMEOUT = 1


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


def test_publish_to_exchange():
    producer = Mock()
    connection = Mock()

    patch_producer = as_mock_property(as_context_manager(producer))
    patch_connection = as_mock_property(as_context_manager(connection))

    with patch('nameko.messaging.maybe_declare') as maybe_declare, \
        patch.object(Publisher, 'producer', patch_producer), \
        patch.object(Publisher, 'connection', patch_connection):

        publisher = Publisher(exchange=foobar_ex)

        # test declarations
        publisher.start()
        maybe_declare.assert_called_once_with(foobar_ex, connection)

        # test publish
        msg = "msg"
        publisher(msg)
        producer.publish.assert_called_once_with(msg, exchange=foobar_ex)


def test_publish_to_queue():
    producer = Mock()
    connection = Mock()

    patch_producer = as_mock_property(as_context_manager(producer))
    patch_connection = as_mock_property(as_context_manager(connection))

    with patch('nameko.messaging.maybe_declare') as maybe_declare, \
        patch.object(Publisher, 'producer', patch_producer), \
        patch.object(Publisher, 'connection', patch_connection):

        publisher = Publisher(queue=foobar_queue)

        # test declarations
        publisher.start()
        maybe_declare.assert_called_once_with(foobar_queue, connection)

        # test publish
        msg = "msg"
        publisher(msg)
        producer.publish.assert_called_once_with(msg, exchange=foobar_ex)


#==============================================================================
# INTEGRATION TESTS
#==============================================================================


def test_publish_to_rabbit(reset_rabbit, rabbit_manager, rabbit_config):

    vhost = rabbit_config['vhost']

    mock_container = Mock()
    mock_container.config = rabbit_config

    publish = Publisher(exchange=foobar_ex, queue=foobar_queue)
    publish.container = mock_container

    # test queue, exchange and binding created in rabbit
    publish.start()
    exchanges = rabbit_manager.get_exchanges(vhost)
    queues = rabbit_manager.get_queues(vhost)
    bindings = rabbit_manager.get_queue_bindings(vhost, foobar_queue.name)

    assert "foobar_ex" in [exchange['name'] for exchange in exchanges]
    assert "foobar_queue" in [queue['name'] for queue in queues]
    assert "foobar_ex" in [binding['source'] for binding in bindings]

    # test message published to queue
    publish("msg")
    messages = rabbit_manager.get_messages(vhost, foobar_queue.name)
    assert ['msg'] == [msg['payload'] for msg in messages]


def test_consume_from_rabbit(reset_rabbit, rabbit_manager, rabbit_config):

    vhost = rabbit_config['vhost']

    mock_container = Mock()
    mock_container.config = rabbit_config

    consumer = ConsumeProvider(queue=foobar_queue, requeue_on_error=False)
    consumer.name = "injection_name"
    consumer.container = mock_container

    # test queue, exchange and binding created in rabbit
    consumer.start()
    consumer.on_container_started()

    exchanges = rabbit_manager.get_exchanges(vhost)
    queues = rabbit_manager.get_queues(vhost)
    bindings = rabbit_manager.get_queue_bindings(vhost, foobar_queue.name)

    assert "foobar_ex" in [exchange['name'] for exchange in exchanges]
    assert "foobar_queue" in [queue['name'] for queue in queues]
    assert "foobar_ex" in [binding['source'] for binding in bindings]

    # test message consumed from queue
    worker = lambda name, args, kwargs, callback: callback("result", None)
    mock_container.spawn_worker.side_effect = worker

    rabbit_manager.publish(vhost, foobar_ex.name, '', 'msg')

    with wait_for_call(CONSUME_TIMEOUT, mock_container.spawn_worker) as method:
        method.assert_called_once_with(consumer.name, ('msg',), {}, ANY)

    # stop will hang if the consumer hasn't acked or requeued messages
    consumer.stop()
