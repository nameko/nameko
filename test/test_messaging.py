import eventlet
from kombu import Exchange, Queue
from mock import patch, Mock

from nameko.dependencies import get_entrypoint_providers
from nameko.messaging import (
    Publisher, ConsumeProvider, consume, HeaderEncoder, HeaderDecoder)
from nameko.service import (
    ServiceContext, WorkerContext, WorkerContextBase, NAMEKO_DATA_KEYS)
from nameko.testing.utils import (
    wait_for_call, as_context_manager, ANY_PARTIAL)

foobar_ex = Exchange('foobar_ex', durable=False)
foobar_queue = Queue('foobar_queue', exchange=foobar_ex, durable=False)

CONSUME_TIMEOUT = 1


class CustomWorkerContext(WorkerContextBase):
    data_keys = NAMEKO_DATA_KEYS + ('customheader',)


def test_consume_creates_provider():
    class Spam(object):
        @consume(queue=foobar_queue)
        def foobar(self):
            pass

    providers = list(get_entrypoint_providers(Spam))
    assert len(providers) == 1

    name, provider = providers[0]
    assert name == 'foobar'
    assert isinstance(provider, ConsumeProvider)
    assert provider.queue == foobar_queue
    assert provider.requeue_on_error is False


def test_consume_provider():

    consume_provider = ConsumeProvider(queue=foobar_queue,
                                       requeue_on_error=False)
    queue_consumer = Mock()
    container = Mock(worker_ctx_cls=WorkerContext)
    message = Mock(headers={})
    srv_ctx = ServiceContext(None, None, container)

    with patch('nameko.messaging.get_queue_consumer') as get_queue_consumer:
        get_queue_consumer.return_value = queue_consumer

        # test lifecycle
        consume_provider.prepare(srv_ctx)
        queue_consumer.add_consumer.assert_called_once_with(
            foobar_queue, ANY_PARTIAL)

        consume_provider.start(srv_ctx)
        queue_consumer.start.assert_called_once_with()

        consume_provider.stop(srv_ctx)
        queue_consumer.stop.assert_called_once_with()

        worker_ctx = WorkerContext(srv_ctx, None, None)

        container.spawn_worker.return_value = worker_ctx

        # test handling successful call
        queue_consumer.reset_mock()
        consume_provider.handle_message(srv_ctx, "body", message)
        handle_result = container.spawn_worker.call_args[1]['handle_result']
        handle_result(worker_ctx, 'result')
        queue_consumer.ack_message.assert_called_once_with(message)

        # test handling failed call without requeue
        queue_consumer.reset_mock()
        consume_provider.requeue_on_error = False
        consume_provider.handle_message(srv_ctx, "body", message)
        handle_result = container.spawn_worker.call_args[1]['handle_result']
        handle_result(worker_ctx, None, Exception('Error'))
        queue_consumer.ack_message.assert_called_once_with(message)

        # test handling failed call with requeue
        queue_consumer.reset_mock()
        consume_provider.requeue_on_error = True
        consume_provider.handle_message(srv_ctx, "body", message)
        handle_result = container.spawn_worker.call_args[1]['handle_result']
        handle_result(worker_ctx, None, Exception('Error'))
        assert not queue_consumer.ack_message.called
        queue_consumer.requeue_message.assert_called_once_with(message)


def test_publish_to_exchange():
    producer = Mock()
    connection = Mock()
    service = Mock()
    srv_ctx = Mock()
    worker_ctx = WorkerContext(srv_ctx, service, None)

    publisher = Publisher(exchange=foobar_ex)
    publisher.name = "publish"

    with patch('nameko.messaging.maybe_declare') as maybe_declare, \
            patch.object(publisher, 'get_connection') as get_connection, \
            patch.object(publisher, 'get_producer') as get_producer:

        get_connection.return_value = as_context_manager(connection)
        get_producer.return_value = as_context_manager(producer)

        # test declarations
        publisher.prepare(srv_ctx)
        maybe_declare.assert_called_once_with(foobar_ex, connection)

        # test publish
        msg = "msg"
        publisher.inject(worker_ctx)
        service.publish(msg)
        producer.publish.assert_called_once_with(msg, headers={},
                                                 exchange=foobar_ex)


def test_publish_to_queue():
    producer = Mock()
    connection = Mock()
    service = Mock()
    srv_ctx = Mock()

    ctx_data = {'language': 'en'}
    worker_ctx = WorkerContext(srv_ctx, service, None, data=ctx_data)

    publisher = Publisher(queue=foobar_queue)
    publisher.name = "publish"

    with patch('nameko.messaging.maybe_declare') as maybe_declare, \
            patch.object(publisher, 'get_connection') as get_connection, \
            patch.object(publisher, 'get_producer') as get_producer:

        get_connection.return_value = as_context_manager(connection)
        get_producer.return_value = as_context_manager(producer)

        # test declarations
        publisher.prepare(srv_ctx)
        maybe_declare.assert_called_once_with(foobar_queue, connection)

        # test publish
        msg = "msg"
        headers = {'nameko.language': 'en'}
        publisher.inject(worker_ctx)
        service.publish(msg)
        producer.publish.assert_called_once_with(msg, headers=headers,
                                                 exchange=foobar_ex)


def test_publish_custom_headers():
    producer = Mock()
    connection = Mock()
    service = Mock()
    srv_ctx = Mock()

    ctx_data = {'language': 'en', 'customheader': 'customvalue'}
    worker_ctx = CustomWorkerContext(srv_ctx, service, None, data=ctx_data)

    publisher = Publisher(queue=foobar_queue)
    publisher.name = "publish"

    with patch('nameko.messaging.maybe_declare') as maybe_declare, \
            patch.object(publisher, 'get_connection') as get_connection, \
            patch.object(publisher, 'get_producer') as get_producer:

        get_connection.return_value = as_context_manager(connection)
        get_producer.return_value = as_context_manager(producer)

        # test declarations
        publisher.prepare(srv_ctx)
        maybe_declare.assert_called_once_with(foobar_queue, connection)

        # test publish
        msg = "msg"
        headers = {'nameko.language': 'en',
                   'nameko.customheader': 'customvalue'}
        publisher.inject(worker_ctx)
        service.publish(msg)
        producer.publish.assert_called_once_with(msg, headers=headers,
                                                 exchange=foobar_ex)


def test_header_encoder():

    context_data = {
        'foo': 'FOO',
        'bar': 'BAR',
        'baz': 'BAZ'
    }

    encoder = HeaderEncoder()
    with patch.object(encoder, 'header_prefix', new="testprefix"):

        worker_ctx = Mock(data_keys=("foo", "bar", "xxx"), data=context_data)

        res = encoder.get_message_headers(worker_ctx)
        assert res == {'testprefix.foo': 'FOO', 'testprefix.bar': 'BAR'}


def test_header_decoder():

    headers = {
        'testprefix.foo': 'FOO',
        'testprefix.bar': 'BAR',
        'testprefix.baz': 'BAZ',
        'bogusprefix.foo': 'XXX'
    }

    decoder = HeaderDecoder()
    with patch.object(decoder, 'header_prefix', new="testprefix"):

        worker_ctx_cls = Mock(data_keys=("foo", "bar"))
        message = Mock(headers=headers)

        res = decoder.unpack_message_headers(worker_ctx_cls, message)
        assert res == {'foo': 'FOO', 'bar': 'BAR'}


# TODO: we need to define the expected behavior for errors raised by
# DeoratorDependencies and add tests to ensure the behavior, e.g. socket errors


#==============================================================================
# INTEGRATION TESTS
#==============================================================================


def test_publish_to_rabbit(reset_rabbit, rabbit_manager, rabbit_config):

    vhost = rabbit_config['vhost']
    service = Mock()
    srv_ctx = ServiceContext(None, None, None, config=rabbit_config)
    ctx_data = {'language': 'en', 'customheader': 'customvalue'}
    worker_ctx = CustomWorkerContext(srv_ctx, service, None, data=ctx_data)

    publisher = Publisher(exchange=foobar_ex, queue=foobar_queue)
    publisher.name = "publish"

    # test queue, exchange and binding created in rabbit
    publisher.prepare(srv_ctx)
    publisher.start(srv_ctx)

    exchanges = rabbit_manager.get_exchanges(vhost)
    queues = rabbit_manager.get_queues(vhost)
    bindings = rabbit_manager.get_queue_bindings(vhost, foobar_queue.name)

    assert "foobar_ex" in [exchange['name'] for exchange in exchanges]
    assert "foobar_queue" in [queue['name'] for queue in queues]
    assert "foobar_ex" in [binding['source'] for binding in bindings]

    # test message published to queue
    publisher.inject(worker_ctx)
    service.publish("msg")
    messages = rabbit_manager.get_messages(vhost, foobar_queue.name)
    assert ['msg'] == [msg['payload'] for msg in messages]

    # test message headers
    assert messages[0]['properties']['headers'] == {
        'nameko.language': 'en',
        'nameko.customheader': 'customvalue'
    }


def test_consume_from_rabbit(reset_rabbit, rabbit_manager, rabbit_config):

    vhost = rabbit_config['vhost']

    mock_container = Mock(worker_ctx_cls=CustomWorkerContext)
    srv_ctx = ServiceContext(None, None, mock_container, config=rabbit_config)

    consumer = ConsumeProvider(queue=foobar_queue, requeue_on_error=False)
    consumer.name = "injection_name"

    # test queue, exchange and binding created in rabbit
    consumer.prepare(srv_ctx)
    consumer.start(srv_ctx)

    exchanges = rabbit_manager.get_exchanges(vhost)
    queues = rabbit_manager.get_queues(vhost)
    bindings = rabbit_manager.get_queue_bindings(vhost, foobar_queue.name)

    assert "foobar_ex" in [exchange['name'] for exchange in exchanges]
    assert "foobar_queue" in [queue['name'] for queue in queues]
    assert "foobar_ex" in [binding['source'] for binding in bindings]

    # test message consumed from queue
    worker_ctx = mock_container.worker_ctx_cls(srv_ctx, None, None)
    mock_container.spawn_worker.return_value = worker_ctx

    headers = {'nameko.language': 'en', 'nameko.customheader': 'customvalue'}
    rabbit_manager.publish(
        vhost, foobar_ex.name, '', 'msg', properties=dict(headers=headers))

    ctx_data = {'language': 'en', 'customheader': 'customvalue'}
    with wait_for_call(CONSUME_TIMEOUT, mock_container.spawn_worker) as method:
        method.assert_called_once_with(consumer, ('msg',), {},
                                       context_data=ctx_data,
                                       handle_result=ANY_PARTIAL)
        handle_result = method.call_args[1]['handle_result']

    # ack message
    handle_result(worker_ctx, 'result')

    # stop will hang if the consumer hasn't acked or requeued messages
    with eventlet.timeout.Timeout(CONSUME_TIMEOUT):
        consumer.stop(srv_ctx)
