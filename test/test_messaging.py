import eventlet
import pytest
from kombu import Exchange, Queue
from mock import Mock, patch

from nameko.constants import DEFAULT_RETRY_POLICY
from nameko.containers import WorkerContext
from nameko.exceptions import ContainerBeingKilled
from nameko.messaging import Consumer, HeaderDecoder, HeaderEncoder, Publisher
from nameko.testing.utils import (
    ANY_PARTIAL, DummyProvider, as_context_manager, wait_for_call)

foobar_ex = Exchange('foobar_ex', durable=False)
foobar_queue = Queue('foobar_queue', exchange=foobar_ex, durable=False)

CONSUME_TIMEOUT = 1


@pytest.yield_fixture
def patch_publisher():

    patchers = []

    def apply_patches(publisher):
        p1 = patch.object(publisher, 'get_connection', autospec=True)
        p2 = patch.object(publisher, 'get_producer', autospec=True)
        patchers.extend((p1, p2))

        return p1.start(), p2.start()

    yield apply_patches

    for patcher in patchers:
        patcher.stop()


@pytest.yield_fixture
def maybe_declare():
    with patch('nameko.messaging.maybe_declare', autospec=True) as patched:
        yield patched


def test_consume_provider(mock_container):

    container = mock_container
    container.shared_extensions = {}
    container.worker_ctx_cls = WorkerContext
    container.service_name = "service"

    worker_ctx = WorkerContext(container, None, DummyProvider())

    spawn_worker = container.spawn_worker
    spawn_worker.return_value = worker_ctx

    queue_consumer = Mock()

    consume_provider = Consumer(
        queue=foobar_queue, requeue_on_error=False).bind(container, "consume")
    consume_provider.queue_consumer = queue_consumer

    message = Mock(headers={})

    # test lifecycle
    consume_provider.setup()
    queue_consumer.register_provider.assert_called_once_with(
        consume_provider)

    consume_provider.stop()
    queue_consumer.unregister_provider.assert_called_once_with(
        consume_provider)

    # test handling successful call
    queue_consumer.reset_mock()
    consume_provider.handle_message("body", message)
    handle_result = spawn_worker.call_args[1]['handle_result']
    handle_result(worker_ctx, 'result')
    queue_consumer.ack_message.assert_called_once_with(message)

    # test handling failed call without requeue
    queue_consumer.reset_mock()
    consume_provider.requeue_on_error = False
    consume_provider.handle_message("body", message)
    handle_result = spawn_worker.call_args[1]['handle_result']
    handle_result(worker_ctx, None, (Exception, Exception('Error'), "tb"))
    queue_consumer.ack_message.assert_called_once_with(message)

    # test handling failed call with requeue
    queue_consumer.reset_mock()
    consume_provider.requeue_on_error = True
    consume_provider.handle_message("body", message)
    handle_result = spawn_worker.call_args[1]['handle_result']
    handle_result(worker_ctx, None, (Exception, Exception('Error'), "tb"))
    assert not queue_consumer.ack_message.called
    queue_consumer.requeue_message.assert_called_once_with(message)

    # test requeueing on ContainerBeingKilled (even without requeue_on_error)
    queue_consumer.reset_mock()
    consume_provider.requeue_on_error = False
    spawn_worker.side_effect = ContainerBeingKilled()
    consume_provider.handle_message("body", message)
    assert not queue_consumer.ack_message.called
    queue_consumer.requeue_message.assert_called_once_with(message)


@pytest.mark.usefixtures("predictable_call_ids")
def test_publish_to_exchange(maybe_declare, patch_publisher, mock_container):
    container = mock_container
    container.service_name = "srcservice"

    service = Mock()
    worker_ctx = WorkerContext(container, service, DummyProvider("publish"))

    publisher = Publisher(exchange=foobar_ex).bind(container, "publish")

    producer = Mock()
    connection = Mock()

    get_connection, get_producer = patch_publisher(publisher)

    get_connection.return_value = as_context_manager(connection)
    get_producer.return_value = as_context_manager(producer)

    # test declarations
    publisher.setup()
    maybe_declare.assert_called_once_with(foobar_ex, connection)

    # test publish
    msg = "msg"
    service.publish = publisher.get_dependency(worker_ctx)
    service.publish(msg, publish_kwarg="value")
    headers = {
        'nameko.call_id_stack': ['srcservice.publish.0']
    }
    producer.publish.assert_called_once_with(
        msg, headers=headers, exchange=foobar_ex, retry=True,
        serializer=container.serializer,
        retry_policy=DEFAULT_RETRY_POLICY, publish_kwarg="value")


@pytest.mark.usefixtures("predictable_call_ids")
def test_publish_to_queue(maybe_declare, patch_publisher, mock_container):
    container = mock_container
    container.shared_extensions = {}
    container.service_name = "srcservice"

    ctx_data = {'language': 'en'}
    service = Mock()
    worker_ctx = WorkerContext(
        container, service, DummyProvider("publish"), data=ctx_data)

    publisher = Publisher(queue=foobar_queue).bind(container, "publish")

    producer = Mock()
    connection = Mock()

    get_connection, get_producer = patch_publisher(publisher)

    get_connection.return_value = as_context_manager(connection)
    get_producer.return_value = as_context_manager(producer)

    # test declarations
    publisher.setup()
    maybe_declare.assert_called_once_with(foobar_queue, connection)

    # test publish
    msg = "msg"
    headers = {
        'nameko.language': 'en',
        'nameko.call_id_stack': ['srcservice.publish.0'],
    }
    service.publish = publisher.get_dependency(worker_ctx)
    service.publish(msg, publish_kwarg="value")
    producer.publish.assert_called_once_with(
        msg, headers=headers, exchange=foobar_ex, retry=True,
        serializer=container.serializer,
        retry_policy=DEFAULT_RETRY_POLICY, publish_kwarg="value")


@pytest.mark.usefixtures("predictable_call_ids")
def test_publish_custom_headers(mock_container, maybe_declare,
                                patch_publisher):

    container = mock_container
    container.service_name = "srcservice"

    ctx_data = {'language': 'en', 'customheader': 'customvalue'}
    service = Mock()
    worker_ctx = WorkerContext(
        container, service, DummyProvider('method'), data=ctx_data
    )

    publisher = Publisher(queue=foobar_queue).bind(container, "publish")

    producer = Mock()
    connection = Mock()

    get_connection, get_producer = patch_publisher(publisher)

    get_connection.return_value = as_context_manager(connection)
    get_producer.return_value = as_context_manager(producer)

    # test declarations
    publisher.setup()
    maybe_declare.assert_called_once_with(foobar_queue, connection)

    # test publish
    msg = "msg"
    headers = {'nameko.language': 'en',
               'nameko.customheader': 'customvalue',
               'nameko.call_id_stack': ['srcservice.method.0']}
    service.publish = publisher.get_dependency(worker_ctx)
    service.publish(msg, publish_kwarg="value")
    producer.publish.assert_called_once_with(
        msg, headers=headers, exchange=foobar_ex, retry=True,
        serializer=container.serializer,
        retry_policy=DEFAULT_RETRY_POLICY, publish_kwarg="value")


def test_header_encoder(empty_config):

    context_data = {
        'foo': 'FOO',
        'bar': 'BAR',
        'baz': 'BAZ',
        # unserialisable, shouldn't be included in the processed headers
        'none': None,
    }

    encoder = HeaderEncoder()
    with patch.object(encoder, 'header_prefix', new="testprefix"):

        worker_ctx = Mock(context_data=context_data)

        res = encoder.get_message_headers(worker_ctx)
        assert res == {
            'testprefix.foo': 'FOO',
            'testprefix.bar': 'BAR',
            'testprefix.baz': 'BAZ',
        }


def test_header_decoder():

    headers = {
        'testprefix.foo': 'FOO',
        'testprefix.bar': 'BAR',
        'testprefix.baz': 'BAZ',
        'differentprefix.foo': 'XXX',
        'testprefix.call_id_stack': ['a', 'b', 'c'],
    }

    decoder = HeaderDecoder()
    with patch.object(decoder, 'header_prefix', new="testprefix"):

        message = Mock(headers=headers)

        res = decoder.unpack_message_headers(None, message)
        assert res == {
            'foo': 'FOO',
            'bar': 'BAR',
            'baz': 'BAZ',
            'call_id_stack': ['a', 'b', 'c'],
            'differentprefix.foo': 'XXX'
        }


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

@pytest.mark.usefixtures("predictable_call_ids")
def test_publish_to_rabbit(rabbit_manager, rabbit_config, mock_container):

    vhost = rabbit_config['vhost']

    container = mock_container
    container.service_name = "service"
    container.config = rabbit_config

    ctx_data = {'language': 'en', 'customheader': 'customvalue'}
    service = Mock()
    worker_ctx = WorkerContext(
        container, service, DummyProvider('method'), data=ctx_data
    )

    publisher = Publisher(
        exchange=foobar_ex, queue=foobar_queue).bind(container, "publish")

    # test queue, exchange and binding created in rabbit
    publisher.setup()
    publisher.start()

    exchanges = rabbit_manager.get_exchanges(vhost)
    queues = rabbit_manager.get_queues(vhost)
    bindings = rabbit_manager.get_queue_bindings(vhost, foobar_queue.name)

    assert "foobar_ex" in [exchange['name'] for exchange in exchanges]
    assert "foobar_queue" in [queue['name'] for queue in queues]
    assert "foobar_ex" in [binding['source'] for binding in bindings]

    # test message published to queue
    service.publish = publisher.get_dependency(worker_ctx)
    service.publish("msg")
    messages = rabbit_manager.get_messages(vhost, foobar_queue.name)
    assert ['"msg"'] == [msg['payload'] for msg in messages]

    # test message headers
    assert messages[0]['properties']['headers'] == {
        'nameko.language': 'en',
        'nameko.customheader': 'customvalue',
        'nameko.call_id_stack': ['service.method.0'],
    }


@pytest.mark.usefixtures("predictable_call_ids")
def test_unserialisable_headers(rabbit_manager, rabbit_config, mock_container):

    vhost = rabbit_config['vhost']

    container = mock_container
    container.service_name = "service"
    container.config = rabbit_config
    container.spawn_managed_thread = eventlet.spawn

    ctx_data = {'language': 'en', 'customheader': None}
    service = Mock()
    worker_ctx = WorkerContext(
        container, service, DummyProvider('method'), data=ctx_data
    )

    publisher = Publisher(
        exchange=foobar_ex, queue=foobar_queue).bind(container, "publish")

    publisher.setup()
    publisher.start()

    service.publish = publisher.get_dependency(worker_ctx)
    service.publish("msg")
    messages = rabbit_manager.get_messages(vhost, foobar_queue.name)

    assert messages[0]['properties']['headers'] == {
        'nameko.language': 'en',
        'nameko.call_id_stack': ['service.method.0'],
        # no `customheader`
    }


def test_consume_from_rabbit(rabbit_manager, rabbit_config, mock_container):

    vhost = rabbit_config['vhost']

    container = mock_container
    container.shared_extensions = {}
    container.worker_ctx_cls = WorkerContext
    container.service_name = "service"
    container.config = rabbit_config
    container.max_workers = 10

    content_type = 'application/data'
    container.accept = [content_type]

    def spawn_managed_thread(method):
        return eventlet.spawn(method)

    container.spawn_managed_thread = spawn_managed_thread

    worker_ctx = WorkerContext(container, None, DummyProvider())

    consumer = Consumer(
        queue=foobar_queue, requeue_on_error=False).bind(container, "publish")

    # prepare and start extensions
    consumer.setup()
    consumer.queue_consumer.setup()
    consumer.start()
    consumer.queue_consumer.start()

    # test queue, exchange and binding created in rabbit
    exchanges = rabbit_manager.get_exchanges(vhost)
    queues = rabbit_manager.get_queues(vhost)
    bindings = rabbit_manager.get_queue_bindings(vhost, foobar_queue.name)

    assert "foobar_ex" in [exchange['name'] for exchange in exchanges]
    assert "foobar_queue" in [queue['name'] for queue in queues]
    assert "foobar_ex" in [binding['source'] for binding in bindings]

    # test message consumed from queue
    container.spawn_worker.return_value = worker_ctx

    headers = {'nameko.language': 'en', 'nameko.customheader': 'customvalue'}

    rabbit_manager.publish(
        vhost, foobar_ex.name, '', 'msg',
        properties=dict(headers=headers, content_type=content_type))

    ctx_data = {
        'language': 'en',
        'customheader': 'customvalue',
    }
    with wait_for_call(CONSUME_TIMEOUT, container.spawn_worker) as method:
        method.assert_called_once_with(consumer, ('msg',), {},
                                       context_data=ctx_data,
                                       handle_result=ANY_PARTIAL)
        handle_result = method.call_args[1]['handle_result']

    # ack message
    handle_result(worker_ctx, 'result')

    # stop will hang if the consumer hasn't acked or requeued messages
    with eventlet.timeout.Timeout(CONSUME_TIMEOUT):
        consumer.stop()

    consumer.queue_consumer.kill()
