import itertools
import time
from collections import defaultdict

import pytest
from mock import ANY, Mock, create_autospec, patch
from six.moves import queue

from nameko.containers import WorkerContext
from nameko.events import (
    BROADCAST, SERVICE_POOL, SINGLETON, EventDispatcher, EventHandler,
    EventHandlerConfigurationError, event_handler
)
from nameko.messaging import QueueConsumer
from nameko.standalone.events import event_dispatcher as standalone_dispatcher
from nameko.standalone.events import get_event_exchange
from nameko.testing.services import dummy, entrypoint_hook, entrypoint_waiter
from nameko.testing.utils import DummyProvider, unpack_mock_call


EVENTS_TIMEOUT = 5


@pytest.yield_fixture
def queue_consumer():
    replacement = create_autospec(QueueConsumer)
    with patch.object(QueueConsumer, 'bind') as mock_ext:
        mock_ext.return_value = replacement
        yield replacement


def test_event_dispatcher(mock_container, mock_producer, rabbit_config):

    container = mock_container
    container.config = rabbit_config
    container.service_name = "srcservice"

    service = Mock()
    worker_ctx = WorkerContext(container, service, DummyProvider("dispatch"))

    custom_retry_policy = {'max_retries': 5}

    event_dispatcher = EventDispatcher(retry_policy=custom_retry_policy).bind(
        container, attr_name="dispatch")
    event_dispatcher.setup()

    service.dispatch = event_dispatcher.get_dependency(worker_ctx)
    service.dispatch('eventtype', 'msg')

    headers = event_dispatcher.get_message_headers(worker_ctx)

    expected_args = ('msg',)
    expected_kwargs = {
        'exchange': ANY,
        'routing_key': 'eventtype',
        'headers': headers,
        'declare': event_dispatcher.declare,
        'retry': event_dispatcher.publisher_cls.retry,
        'retry_policy': custom_retry_policy,
        'compression': event_dispatcher.publisher_cls.compression,
        'mandatory': event_dispatcher.publisher_cls.mandatory,
        'expiration': event_dispatcher.publisher_cls.expiration,
        'delivery_mode': event_dispatcher.publisher_cls.delivery_mode,
        'priority': event_dispatcher.publisher_cls.priority,
        'serializer': event_dispatcher.serializer,
    }

    assert mock_producer.publish.call_count == 1
    args, kwargs = mock_producer.publish.call_args
    assert args == expected_args
    assert kwargs == expected_kwargs
    assert kwargs['exchange'].name == 'srcservice.events'


def test_event_handler(queue_consumer, mock_container):

    container = mock_container
    container.service_name = "destservice"

    # test default configuration
    event_handler = EventHandler("srcservice", "eventtype").bind(container,
                                                                 "foobar")
    event_handler.setup()

    assert event_handler.queue.durable is True
    assert event_handler.queue.routing_key == "eventtype"
    assert event_handler.queue.exchange.name == "srcservice.events"
    queue_consumer.register_provider.assert_called_once_with(event_handler)

    # test service pool handler
    event_handler = EventHandler(
        "srcservice", "eventtype"
    ).bind(
        container, "foobar"
    )
    event_handler.setup()

    assert event_handler.queue.name == (
        "evt-srcservice-eventtype--destservice.foobar")
    assert event_handler.queue.exclusive is False

    # test broadcast handler with default identifier
    with patch('nameko.events.uuid') as mock_uuid:
        mock_uuid.uuid4().hex = "uuid-value"
        event_handler = EventHandler(
            "srcservice", "eventtype",
            handler_type=BROADCAST, reliable_delivery=False
        ).bind(
            container, "foobar"
        )
        event_handler.setup()

    assert event_handler.queue.name == (
        "evt-srcservice-eventtype--destservice.foobar-{}".format("uuid-value"))
    assert event_handler.queue.exclusive is True

    # test broadcast handler with custom identifier
    class BroadcastEventHandler(EventHandler):
        broadcast_identifier = "testbox"

    event_handler = BroadcastEventHandler(
        "srcservice", "eventtype", handler_type=BROADCAST
    ).bind(
        container, "foobar"
    )
    event_handler.setup()

    assert event_handler.queue.name == (
        "evt-srcservice-eventtype--destservice.foobar-{}".format("testbox"))
    assert event_handler.queue.exclusive is False

    # test singleton handler
    event_handler = EventHandler(
        "srcservice", "eventtype", handler_type=SINGLETON
    ).bind(
        container, "foobar"
    )
    event_handler.setup()

    assert event_handler.queue.name == "evt-srcservice-eventtype"
    assert event_handler.queue.exclusive is False

    # test reliable delivery
    event_handler = EventHandler(
        "srcservice", "eventtype"
    ).bind(
        container, "foobar"
    )
    event_handler.setup()

    assert event_handler.queue.auto_delete is False


class TestReliableDeliveryEventHandlerConfigurationError():

    def test_raises_with_default_broadcast_identity(
        self, queue_consumer, mock_container
    ):

        container = mock_container
        container.service_name = "destservice"

        # test broadcast handler with reliable delivery
        with pytest.raises(EventHandlerConfigurationError):
            EventHandler(
                "srcservice", "eventtype",
                handler_type=BROADCAST, reliable_delivery=True
            ).bind(
                container, "foobar"
            )

    def test_no_raise_with_custom_identity(
        self, queue_consumer, mock_container
    ):
        container = mock_container
        container.service_name = "destservice"

        # test broadcast handler with reliable delivery and custom identifier
        class BroadcastEventHandler(EventHandler):
            broadcast_identifier = "testbox"

        event_handler = BroadcastEventHandler(
            "srcservice", "eventtype",
            handler_type=BROADCAST, reliable_delivery=True
        ).bind(
            container, "foobar"
        )
        assert event_handler.reliable_delivery is True


# =============================================================================
# INTEGRATION TESTS
# =============================================================================


services = defaultdict(list)
events = []


@pytest.yield_fixture
def reset_state():
    yield
    services.clear()
    events[:] = []


class CustomEventHandler(EventHandler):
    _calls = []

    def __init__(self, *args, **kwargs):
        super(CustomEventHandler, self).__init__(*args, **kwargs)
        self._calls[:] = []

    def handle_result(self, message, worker_ctx, result=None, exc_info=None):
        super(CustomEventHandler, self).handle_result(
            message, worker_ctx, result, exc_info)
        self._calls.append(message)
        return result, exc_info


custom_event_handler = CustomEventHandler.decorator


class HandlerService(object):
    """ Generic service that handles events.
    """
    name = "handlerservice"

    def __init__(self):
        self.events = []
        services[self.name].append(self)

    def handle(self, evt):
        self.events.append(evt)
        events.append(evt)


class ServicePoolHandler(HandlerService):

    @event_handler('srcservice', 'eventtype', handler_type=SERVICE_POOL)
    def handle(self, evt):
        super(ServicePoolHandler, self).handle(evt)


class DoubleServicePoolHandler(HandlerService):

    @event_handler('srcservice', 'eventtype', handler_type=SERVICE_POOL)
    def handle_1(self, evt):
        super(DoubleServicePoolHandler, self).handle(evt)

    @event_handler('srcservice', 'eventtype', handler_type=SERVICE_POOL)
    def handle_2(self, evt):
        super(DoubleServicePoolHandler, self).handle(evt)


class SingletonHandler(HandlerService):

    @event_handler('srcservice', 'eventtype', handler_type=SINGLETON)
    def handle(self, evt):
        super(SingletonHandler, self).handle(evt)


class BroadcastHandler(HandlerService):

    @event_handler(
        'srcservice', 'eventtype',
        handler_type=BROADCAST, reliable_delivery=False
    )
    def handle(self, evt):
        super(BroadcastHandler, self).handle(evt)


class RequeueingHandler(HandlerService):

    @event_handler('srcservice', 'eventtype', requeue_on_error=True)
    def handle(self, evt):
        super(RequeueingHandler, self).handle(evt)
        raise Exception("Error")


class UnreliableHandler(HandlerService):

    @event_handler('srcservice', 'eventtype', reliable_delivery=False)
    def handle(self, evt):
        super(UnreliableHandler, self).handle(evt)


class CustomHandler(HandlerService):
    @custom_event_handler('srcservice', 'eventtype')
    def handle(self, evt):
        super(CustomHandler, self).handle(evt)


def service_factory(prefix, base):
    """ Test utility to create subclasses of the above ServiceHandler classes
    based on a prefix and base. The prefix is set as the ``name`` attribute
    on the resulting type.

    e.g. ``service_factory("foo", ServicePoolHandler)`` returns a type
    called ``FooServicePoolHandler`` that inherits from ``ServicePoolHandler``,
    and ``FooServicePoolHandler.name`` is ``"foo"``.
    """
    name = prefix.title() + base.__name__
    cls = type(name, (base,), {'name': prefix})
    return cls


@pytest.fixture
def start_containers(request, container_factory, rabbit_config, reset_state):
    def make(base, prefixes):
        """ Use ``service_factory`` to create a service type inheriting from
        ``base`` using the given prefixes, and start a container for that
        service.

        If a prefix is given multiple times, create multiple containers for
        that service type. If no prefixes are given, create a single container
        with a type that does not extend the base.

        Stops all started containers when the test ends.
        """
        services = {}
        containers = []
        for prefix in prefixes:
            key = (prefix, base)
            if key not in services:
                service = service_factory(prefix, base)
                services[key] = service
            service_cls = services.get(key)
            ct = container_factory(service_cls, rabbit_config)
            containers.append(ct)
            ct.start()

            request.addfinalizer(ct.stop)

        return containers
    return make


def test_service_pooled_events(rabbit_manager, rabbit_config,
                               start_containers):
    vhost = rabbit_config['vhost']
    start_containers(ServicePoolHandler, ("foo", "foo", "bar"))

    # foo service pool queue should have two consumers
    foo_queue = rabbit_manager.get_queue(
        vhost, "evt-srcservice-eventtype--foo.handle")
    assert len(foo_queue['consumer_details']) == 2

    # bar service pool queue should have one consumer
    bar_queue = rabbit_manager.get_queue(
        vhost, "evt-srcservice-eventtype--bar.handle")
    assert len(bar_queue['consumer_details']) == 1

    exchange_name = "srcservice.events"
    rabbit_manager.publish(
        vhost, exchange_name, 'eventtype', '"msg"',
        properties=dict(content_type='application/json')
    )
    # can't use the entrypoint waiter here because we don't know
    # which of the "foo" containers will pick up the message,
    # so just wait for events to propagate
    time.sleep(.1)

    # a total of two events should be received
    assert len(events) == 2

    # exactly one instance of each service should have been created
    # each should have received an event
    assert len(services['foo']) == 1
    assert isinstance(services['foo'][0], ServicePoolHandler)
    assert services['foo'][0].events == ["msg"]

    assert len(services['bar']) == 1
    assert isinstance(services['bar'][0], ServicePoolHandler)
    assert services['bar'][0].events == ["msg"]


def test_service_pooled_events_multiple_handlers(
        rabbit_manager, rabbit_config, start_containers):

    vhost = rabbit_config['vhost']
    (container,) = start_containers(DoubleServicePoolHandler, ("double",))

    # we should have two queues with a consumer each
    foo_queue_1 = rabbit_manager.get_queue(
        vhost, "evt-srcservice-eventtype--double.handle_1")
    assert len(foo_queue_1['consumer_details']) == 1

    foo_queue_2 = rabbit_manager.get_queue(
        vhost, "evt-srcservice-eventtype--double.handle_2")
    assert len(foo_queue_2['consumer_details']) == 1

    exchange_name = "srcservice.events"

    with entrypoint_waiter(container, 'handle_1'):
        with entrypoint_waiter(container, 'handle_2'):
            rabbit_manager.publish(
                vhost, exchange_name, 'eventtype', '"msg"',
                properties=dict(content_type='application/json')
            )

    # each handler (3 of them) of the two services should have received the evt
    assert len(events) == 2

    # two worker instances would have been created to deal with the handling
    assert len(services['double']) == 2
    assert services['double'][0].events == ["msg"]
    assert services['double'][1].events == ["msg"]


def test_singleton_events(rabbit_manager, rabbit_config, start_containers):

    vhost = rabbit_config['vhost']
    start_containers(SingletonHandler, ("foo", "foo", "bar"))

    # the singleton queue should have three consumers
    queue = rabbit_manager.get_queue(vhost, "evt-srcservice-eventtype")
    assert len(queue['consumer_details']) == 3

    exchange_name = "srcservice.events"
    rabbit_manager.publish(vhost, exchange_name, 'eventtype', '"msg"',
                           properties=dict(content_type='application/json'))

    # can't use the entrypoint waiter here because we don't know
    # which of the containers will pick up the message,
    # so just wait for events to propagate
    time.sleep(.1)

    # exactly one event should have been received
    assert len(events) == 1

    # one lucky handler should have received the event
    assert len(services) == 1
    lucky_service = next(iter(services))
    assert len(services[lucky_service]) == 1
    assert isinstance(services[lucky_service][0], SingletonHandler)
    assert services[lucky_service][0].events == ["msg"]


def test_broadcast_events(rabbit_manager, rabbit_config, start_containers):
    vhost = rabbit_config['vhost']
    (c1, c2, c3) = start_containers(BroadcastHandler, ("foo", "foo", "bar"))

    # each broadcast queue should have one consumer
    queues = rabbit_manager.get_queues(vhost)
    queue_names = [queue['name'] for queue in queues
                   if queue['name'].startswith("evt-srcservice-eventtype-")]

    assert len(queue_names) == 3
    for name in queue_names:
        queue = rabbit_manager.get_queue(vhost, name)
        assert len(queue['consumer_details']) == 1

    exchange_name = "srcservice.events"

    with entrypoint_waiter(c1, 'handle'):
        with entrypoint_waiter(c2, 'handle'):
            with entrypoint_waiter(c3, 'handle'):
                rabbit_manager.publish(
                    vhost, exchange_name, 'eventtype', '"msg"',
                    properties=dict(content_type='application/json')
                )

    # a total of three events should be received
    assert len(events) == 3

    # all three handlers should receive the event, but they're only of two
    # different types
    assert len(services) == 2

    # two of them were "foo" handlers
    assert len(services['foo']) == 2
    assert isinstance(services['foo'][0], BroadcastHandler)
    assert isinstance(services['foo'][1], BroadcastHandler)

    # and they both should have received the event
    assert services['foo'][0].events == ["msg"]
    assert services['foo'][1].events == ["msg"]

    # the other was a "bar" handler
    assert len(services['bar']) == 1
    assert isinstance(services['bar'][0], BroadcastHandler)

    # and it too should have received the event
    assert services['bar'][0].events == ["msg"]


def test_requeue_on_error(rabbit_manager, rabbit_config, start_containers):
    vhost = rabbit_config['vhost']
    (container,) = start_containers(RequeueingHandler, ('requeue',))

    # the queue should been created and have one consumer
    queue = rabbit_manager.get_queue(
        vhost, "evt-srcservice-eventtype--requeue.handle")
    assert len(queue['consumer_details']) == 1

    counter = itertools.count(start=1)

    def entrypoint_fired_twice(worker_ctx, res, exc_info):
        return next(counter) > 1

    with entrypoint_waiter(
        container, 'handle', callback=entrypoint_fired_twice
    ):
        rabbit_manager.publish(
            vhost, "srcservice.events", 'eventtype', '"msg"',
            properties=dict(content_type='application/json')
        )

    # stop container to make sure the assertions below aren't made in the
    # middle of processing an event
    container.stop()

    # the event will be received multiple times as it gets requeued and then
    # consumed again
    assert len(events) > 1

    # multiple instances of the service should have been instantiated
    assert len(services['requeue']) > 1

    # each instance should have received one event
    for service in services['requeue']:
        assert service.events == ["msg"]


def test_reliable_delivery(
    rabbit_manager, rabbit_config, start_containers, container_factory
):
    """ Events sent to queues declared by ``reliable_delivery`` handlers
    should be received even if no service was listening when they were
    dispatched.
    """
    vhost = rabbit_config['vhost']

    (container,) = start_containers(ServicePoolHandler, ('service-pool',))

    # test queue created, with one consumer
    queue_name = "evt-srcservice-eventtype--service-pool.handle"
    queue = rabbit_manager.get_queue(vhost, queue_name)
    assert len(queue['consumer_details']) == 1

    # publish an event
    exchange_name = "srcservice.events"
    with entrypoint_waiter(container, 'handle'):
        rabbit_manager.publish(
            vhost, exchange_name, 'eventtype', '"msg_1"',
            properties=dict(content_type='application/json')
        )

    # wait for the event to be received
    assert events == ["msg_1"]

    # stop container, check queue still exists, without consumers
    container.stop()
    queues = rabbit_manager.get_queues(vhost)
    assert queue_name in [q['name'] for q in queues]
    queue = rabbit_manager.get_queue(vhost, queue_name)
    assert len(queue['consumer_details']) == 0

    # publish another event while nobody is listening
    rabbit_manager.publish(vhost, exchange_name, 'eventtype', '"msg_2"',
                           properties=dict(content_type='application/json'))

    # verify the message gets queued
    messages = rabbit_manager.get_messages(vhost, queue_name, requeue=True)
    assert ['"msg_2"'] == [msg['payload'] for msg in messages]

    # start another container
    class ServicePool(ServicePoolHandler):
        name = "service-pool"

    container = container_factory(ServicePool, rabbit_config)
    with entrypoint_waiter(container, 'handle'):
        container.start()

    # check the new service to collects the pending event
    assert len(events) == 2
    assert events == ["msg_1", "msg_2"]


def test_unreliable_delivery(rabbit_manager, rabbit_config, start_containers):
    """ Events sent to queues declared by non- ``reliable_delivery`` handlers
    should be lost if no service was listening when they were dispatched.
    """
    vhost = rabbit_config['vhost']

    (c1,) = start_containers(UnreliableHandler, ('unreliable',))

    # start a normal handler service so the auto-delete events exchange
    # for srcservice is not removed when we stop the UnreliableHandler
    (c2,) = start_containers(ServicePoolHandler, ('keep-exchange-alive',))

    # test queue created, with one consumer
    queue_name = "evt-srcservice-eventtype--unreliable.handle"
    queue = rabbit_manager.get_queue(vhost, queue_name)
    assert len(queue['consumer_details']) == 1

    # publish an event
    exchange_name = "srcservice.events"
    with entrypoint_waiter(c1, 'handle'):
        with entrypoint_waiter(c2, 'handle'):
            rabbit_manager.publish(
                vhost, exchange_name, 'eventtype', '"msg_1"',
                properties=dict(content_type='application/json')
            )

    # verify it arrived in both services
    assert events == ["msg_1", "msg_1"]

    # test that the unreliable service received it
    assert len(services['unreliable']) == 1
    assert services['unreliable'][0].events == ["msg_1"]

    # stop container, test queue deleted
    c1.stop()
    queues = rabbit_manager.get_queues(vhost)
    assert queue_name not in [q['name'] for q in queues]

    # publish a second event while nobody is listening
    rabbit_manager.publish(vhost, exchange_name, 'eventtype', '"msg_2"',
                           properties=dict(content_type='application/json'))

    # start another container
    (c3,) = start_containers(UnreliableHandler, ('unreliable',))

    # verify the queue is recreated, with one consumer
    queue = rabbit_manager.get_queue(vhost, queue_name)
    assert len(queue['consumer_details']) == 1

    # publish a third event
    with entrypoint_waiter(c3, 'handle'):
        rabbit_manager.publish(
            vhost, exchange_name, 'eventtype', '"msg_3"',
            properties=dict(content_type='application/json')
        )

    # verify that the "unreliable" handler didn't receive the message sent
    # when there wasn't an instance running
    assert len(services['unreliable']) == 2
    assert services['unreliable'][0].events == ["msg_1"]
    assert services['unreliable'][1].events == ["msg_3"]


def test_custom_event_handler(rabbit_manager, rabbit_config, start_containers):
    """Uses a custom handler subclass for the event_handler entrypoint"""

    (container,) = start_containers(CustomHandler, ('custom-events',))

    payload = {'custom': 'data'}
    dispatch = standalone_dispatcher(rabbit_config)

    with entrypoint_waiter(container, 'handle'):
        dispatch('srcservice', "eventtype", payload)

    assert CustomEventHandler._calls[0].payload == payload


def test_dispatch_to_rabbit(rabbit_manager, rabbit_config, mock_container):

    vhost = rabbit_config['vhost']

    container = mock_container
    container.shared_extensions = {}
    container.service_name = "srcservice"
    container.config = rabbit_config

    service = Mock()
    worker_ctx = WorkerContext(container, service, DummyProvider())

    dispatcher = EventDispatcher().bind(container, 'dispatch')
    dispatcher.setup()
    dispatcher.start()

    # we should have an exchange but no queues
    exchanges = rabbit_manager.get_exchanges(vhost)
    queues = rabbit_manager.get_queues(vhost)
    assert "srcservice.events" in [exchange['name'] for exchange in exchanges]
    assert queues == []

    # manually add a queue to capture the events
    rabbit_manager.create_queue(vhost, "event-sink", auto_delete=True)
    rabbit_manager.create_queue_binding(
        vhost, "srcservice.events", "event-sink", routing_key="eventtype")

    service.dispatch = dispatcher.get_dependency(worker_ctx)
    service.dispatch("eventtype", "msg")

    # test event receieved on manually added queue
    messages = rabbit_manager.get_messages(vhost, "event-sink")
    assert ['"msg"'] == [msg['payload'] for msg in messages]


class TestBackwardsCompatClassAttrs(object):

    @pytest.mark.parametrize("parameter,value", [
        ('retry', False),
        ('retry_policy', {'max_retries': 999}),
        ('use_confirms', False),
    ])
    def test_attrs_are_applied_as_defaults(
        self, parameter, value, mock_container
    ):
        """ Verify that you can specify some fields by subclassing the
        EventDispatcher DependencyProvider.
        """
        dispatcher_cls = type(
            "LegacyEventDispatcher", (EventDispatcher,), {parameter: value}
        )
        with patch('nameko.messaging.warnings') as warnings:
            mock_container.config = {'AMQP_URI': 'memory://'}
            mock_container.service_name = "service"
            dispatcher = dispatcher_cls().bind(mock_container, "dispatch")
        assert warnings.warn.called
        call_args = warnings.warn.call_args
        assert parameter in unpack_mock_call(call_args).positional[0]

        dispatcher.setup()
        assert getattr(dispatcher.publisher, parameter) == value


class TestConfigurability(object):
    """
    Test and demonstrate configuration options for the EventDispatcher
    """

    @pytest.yield_fixture
    def get_producer(self):
        with patch('nameko.amqp.publish.get_producer') as get_producer:
            yield get_producer

    @pytest.fixture
    def producer(self, get_producer):
        producer = get_producer().__enter__.return_value
        # make sure we don't raise UndeliverableMessage if mandatory is True
        producer.channel.returned_messages.get_nowait.side_effect = queue.Empty
        return producer

    @pytest.mark.parametrize("parameter", [
        # delivery options
        'delivery_mode', 'mandatory', 'priority', 'expiration',
        # message options
        'serializer', 'compression',
        # retry policy
        'retry', 'retry_policy',
        # other arbitrary publish kwargs
        'correlation_id', 'user_id', 'bogus_param'
    ])
    def test_regular_parameters(
        self, parameter, mock_container, producer
    ):
        """ Verify that most parameters can be specified at instantiation time.
        """
        mock_container.config = {'AMQP_URI': 'memory://localhost'}
        mock_container.service_name = "service"

        worker_ctx = Mock()
        worker_ctx.context_data = {}

        value = Mock()

        dispatcher = EventDispatcher(
            **{parameter: value}
        ).bind(mock_container, "dispatch")
        dispatcher.setup()

        dispatch = dispatcher.get_dependency(worker_ctx)

        dispatch("event-type", "event-data")
        assert producer.publish.call_args[1][parameter] == value

    @pytest.mark.usefixtures('predictable_call_ids')
    def test_headers(self, mock_container, producer):
        """ Headers can be provided at instantiation time, and are merged with
        Nameko headers.
        """
        mock_container.config = {
            'AMQP_URI': 'memory://localhost'
        }
        mock_container.service_name = "service"

        # use a real worker context so nameko headers are generated
        service = Mock()
        entrypoint = Mock(method_name="method")
        worker_ctx = WorkerContext(
            mock_container, service, entrypoint, data={'context': 'data'}
        )

        nameko_headers = {
            'nameko.context': 'data',
            'nameko.call_id_stack': ['service.method.0'],
        }

        value = {'foo': Mock()}

        dispatcher = EventDispatcher(
            **{'headers': value}
        ).bind(mock_container, "dispatch")
        dispatcher.setup()

        dispatch = dispatcher.get_dependency(worker_ctx)

        def merge_dicts(base, *updates):
            merged = base.copy()
            [merged.update(update) for update in updates]
            return merged

        dispatch("event-type", "event-data")
        assert producer.publish.call_args[1]['headers'] == merge_dicts(
            nameko_headers, value
        )

    def test_restricted_parameters(
        self, mock_container, producer
    ):
        """ Verify that providing routing parameters at instantiation
        time has no effect.
        """
        mock_container.config = {'AMQP_URI': 'memory://localhost'}
        mock_container.service_name = "service"

        worker_ctx = Mock()
        worker_ctx.context_data = {}

        exchange = Mock()
        routing_key = Mock()

        dispatcher = EventDispatcher(
            exchange=exchange,
            routing_key=routing_key,
        ).bind(mock_container, "dispatch")
        dispatcher.setup()

        dispatch = dispatcher.get_dependency(worker_ctx)

        event_exchange = get_event_exchange("service")
        event_type = "event-type"

        dispatch(event_type, "event-data")

        assert producer.publish.call_args[1]['exchange'] == event_exchange
        assert producer.publish.call_args[1]['routing_key'] == event_type


class TestSSL(object):

    @pytest.fixture(params=[True, False])
    def rabbit_ssl_config(self, request, rabbit_ssl_config):
        verify_certs = request.param
        if verify_certs is False:
            # remove certificate paths from config
            rabbit_ssl_config['AMQP_SSL'] = True
        return rabbit_ssl_config

    def test_event_handler_over_ssl(
        self, container_factory, rabbit_ssl_config, rabbit_config
    ):
        class Service(object):
            name = "service"

            @event_handler("service", "event")
            def echo(self, event_data):
                return event_data

        container = container_factory(Service, rabbit_ssl_config)
        container.start()

        dispatch = standalone_dispatcher(rabbit_config)

        with entrypoint_waiter(container, 'echo') as result:
            dispatch("service", "event", "payload")
        assert result.get() == "payload"

    def test_event_dispatcher_over_ssl(
        self, container_factory, rabbit_ssl_config, rabbit_config
    ):
        class Dispatcher(object):
            name = "dispatch"

            dispatch = EventDispatcher()

            @dummy
            def method(self, payload):
                return self.dispatch("event-type", payload)

        class Handler(object):
            name = "handler"

            @event_handler("dispatch", "event-type")
            def echo(self, payload):
                return payload

        dispatcher = container_factory(Dispatcher, rabbit_ssl_config)
        dispatcher.start()

        handler = container_factory(Handler, rabbit_config)
        handler.start()

        with entrypoint_waiter(handler, 'echo') as result:
            with entrypoint_hook(dispatcher, 'method') as dispatch:
                dispatch("payload")
        assert result.get() == "payload"
