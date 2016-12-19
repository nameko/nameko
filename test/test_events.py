import itertools
import time
from collections import defaultdict

import pytest
from mock import ANY, Mock, create_autospec, patch

from nameko.amqp import UndeliverableMessage
from nameko.containers import WorkerContext
from nameko.events import (
    BROADCAST, SERVICE_POOL, SINGLETON, EventDispatcher, EventHandler,
    EventHandlerConfigurationError, event_handler)
from nameko.messaging import QueueConsumer
from nameko.standalone.events import event_dispatcher as standalone_dispatcher
from nameko.testing.services import entrypoint_waiter, dummy, entrypoint_hook
from nameko.testing.utils import DummyProvider


EVENTS_TIMEOUT = 5


@pytest.yield_fixture
def queue_consumer():
    replacement = create_autospec(QueueConsumer)
    with patch.object(QueueConsumer, 'bind') as mock_ext:
        mock_ext.return_value = replacement
        yield replacement


def test_event_dispatcher(mock_container, mock_producer):

    container = mock_container
    container.service_name = "srcservice"

    service = Mock()
    worker_ctx = WorkerContext(container, service, DummyProvider("dispatch"))

    event_dispatcher = EventDispatcher(retry_policy={'max_retries': 5}).bind(
        container, attr_name="dispatch")
    event_dispatcher.setup()

    service.dispatch = event_dispatcher.get_dependency(worker_ctx)
    service.dispatch('eventtype', 'msg')

    headers = event_dispatcher.get_message_headers(worker_ctx)

    mock_producer.publish.assert_called_once_with(
        'msg', exchange=ANY, headers=headers,
        serializer=container.serializer,
        routing_key='eventtype', retry=True, mandatory=False,
        retry_policy={'max_retries': 5})

    _, call_kwargs = mock_producer.publish.call_args
    exchange = call_kwargs['exchange']
    assert exchange.name == 'srcservice.events'


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

    counter = itertools.count()

    def entrypoint_fired_twice(worker_ctx, res, exc_info):
        return next(counter) > 1

    with entrypoint_waiter(
        container, 'handle', callback=entrypoint_fired_twice
    ):
        rabbit_manager.publish(
            vhost, "srcservice.events", 'eventtype', '"msg"',
            properties=dict(content_type='application/json')
        )

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


class TestMandatoryDelivery(object):
    """ Test and demonstrate the mandatory delivery flag.

    Dispatching an event should raise an exception when mandatory delivery
    is requested and there is no destination queue, as long as publish-confirms
    are enabled.
    """

    def test_default(self, container_factory, rabbit_config):

        class Service(object):
            name = "dispatcher"

            dispatch = EventDispatcher()

            @dummy
            def method(self, *args, **kwargs):
                self.dispatch(*args, **kwargs)

        container = container_factory(Service, rabbit_config)
        container.start()

        # events are not mandatory by default;
        # no error when routing to a non-existent handler
        with entrypoint_hook(container, 'method') as dispatch:
            dispatch("event", "payload")

    def test_mandatory_delivery(self, container_factory, rabbit_config):

        class Service(object):
            name = "dispatcher"

            dispatch = EventDispatcher(mandatory=True)

            @dummy
            def method(self, *args, **kwargs):
                self.dispatch(*args, **kwargs)

        container = container_factory(Service, rabbit_config)
        container.start()

        # requesting mandatory delivery will result in an exception
        # if there is no bound queue to receive the message
        with pytest.raises(UndeliverableMessage):
            with entrypoint_hook(container, 'method') as publish:
                publish("event", "payload")

    @patch('nameko.standalone.events.warnings')
    def test_confirms_disabled(
        self, warnings, container_factory, rabbit_config
    ):
        class Service(object):
            name = "dispatcher"

            dispatch = EventDispatcher(mandatory=True, use_confirms=False)

            @dummy
            def method(self, *args, **kwargs):
                self.dispatch(*args, **kwargs)

        container = container_factory(Service, rabbit_config)
        container.start()

        # no exception will be raised if confirms are disabled,
        # even when mandatory delivery is requested,
        # but there will be a warning raised
        with entrypoint_hook(container, 'method') as dispatch:
            dispatch("event", "payload")
        assert warnings.warn.called
