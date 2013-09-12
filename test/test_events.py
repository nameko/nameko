import pytest
import eventlet
from collections import defaultdict

from mock import Mock, patch

from nameko.events import (
    EventDispatcher, Event, EventTypeTooLong, EventTypeMissing,
    EventHandlerConfigurationError, event_handler, SINGLETON, BROADCAST,
    SERVICE_POOL, EventHandler)
from nameko.dependencies import DECORATOR_PROVIDERS_ATTR
from nameko.service import ServiceContext, WorkerContext
from nameko.testing.utils import ANY_PARTIAL, as_context_manager


EVENTS_TIMEOUT = 5


def test_event_type_missing():
    with pytest.raises(EventTypeMissing):
        class MyEvent(Event):
            pass

        MyEvent('spam')


def test_event_type_too_long():
    with pytest.raises(EventTypeTooLong):
        class MyEvent(Event):
            type = 't' * 256

        MyEvent('spam')


def test_reliable_broadcast_config_error():
    with pytest.raises(EventHandlerConfigurationError):
        @event_handler(
            'foo', 'bar', reliable_delivery=True, handler_type=BROADCAST)
        def foo():
            pass


def test_event_handler_decorator():
    """ Verify that the event_handler decorator generates an EventProvider
    """
    decorator = event_handler("servicename", "eventtype")
    handler = decorator(lambda: None)
    provider = list(getattr(handler, DECORATOR_PROVIDERS_ATTR))[0]
    assert isinstance(provider, EventHandler)


def test_event_dispatcher():

    event_dispatcher = EventDispatcher()
    event_dispatcher.name = "dispatch"

    producer = Mock()
    service = Mock()

    srv_ctx = ServiceContext('srcservice', None, None)
    worker_ctx = WorkerContext(srv_ctx, service, "dispatch")

    with patch('nameko.messaging.Publisher.start') as super_start:

        # test start method
        event_dispatcher.start(srv_ctx)
        assert event_dispatcher.exchange.name == "srcservice.events"
        super_start.assert_called_once_with(srv_ctx)

    evt = Mock(type="eventtype", data="msg")
    event_dispatcher.call_setup(worker_ctx)

    with patch.object(event_dispatcher, 'get_producer') as get_producer:
        get_producer.return_value = as_context_manager(producer)

        # test dispatch
        service.dispatch(evt)
        producer.publish.assert_called_once_with(
            evt.data, exchange=event_dispatcher.exchange, routing_key=evt.type)


@pytest.fixture
def handler_factory(request):
    """ Test utility to build EventHandler objects with sensible defaults.

    Also injects ``service`` into the dependency as registering with a
    container would have.
    """
    def make_handler(service_name="srcservice", event_type="eventtype",
                     handler_type=SERVICE_POOL, reliable_delivery=True,
                     requeue_on_error=False):
        return EventHandler(service_name, event_type, handler_type,
                            reliable_delivery, requeue_on_error)
    return make_handler


def test_event_handler(handler_factory):

    queue_consumer = Mock()
    srv_ctx = ServiceContext('destservice', None, None)

    with patch('nameko.messaging.get_queue_consumer') as get_queue_consumer:
        get_queue_consumer.return_value = queue_consumer

        # test default configuration
        event_handler = handler_factory()
        event_handler.start(srv_ctx)
        assert event_handler.queue.durable is True
        assert event_handler.queue.routing_key == "eventtype"
        assert event_handler.queue.exchange.name == "srcservice.events"
        queue_consumer.add_consumer.assert_called_once_with(
            event_handler.queue, ANY_PARTIAL)

        # test service pool handler
        event_handler = handler_factory(handler_type=SERVICE_POOL)
        event_handler.name = 'foobar'
        event_handler.start(srv_ctx)

        assert (event_handler.queue.name ==
                "evt-srcservice-eventtype--destservice.foobar")

        # test broadcast handler
        event_handler = handler_factory(handler_type=BROADCAST)
        event_handler.start(srv_ctx)
        assert event_handler.queue.name.startswith("evt-srcservice-eventtype-")

        # test singleton handler
        event_handler = handler_factory(handler_type=SINGLETON)
        event_handler.start(srv_ctx)
        assert event_handler.queue.name == "evt-srcservice-eventtype"

        # test reliable delivery
        event_handler = handler_factory(reliable_delivery=True)
        event_handler.start(srv_ctx)
        assert event_handler.queue.auto_delete is False


#==============================================================================
# INTEGRATION TESTS
#==============================================================================


services = defaultdict(list)
events = []


@pytest.fixture
def reset_state():
    services.clear()
    events[:] = []


class ExampleEvent(Event):
    type = "eventtype"


class HandlerService(object):
    """ Generic service that handles events.
    """
    name = "srvname"

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

    @event_handler('srcservice', 'eventtype', handler_type=BROADCAST,
                   reliable_delivery=False)
    def handle(self, evt):
        super(BroadcastHandler, self).handle(evt)


class RequeueingHandler(HandlerService):
    name = "requeue"

    @event_handler('srcservice', 'eventtype', requeue_on_error=True)
    def handle(self, evt):
        super(RequeueingHandler, self).handle(evt)
        raise Exception("Error")


class UnreliableHandler(HandlerService):
    name = "unreliable"

    @event_handler('srcservice', 'eventtype', reliable_delivery=False)
    def handle(self, evt):
        super(UnreliableHandler, self).handle(evt)


def service_factory(prefix, base):
    """ Test utility to create subclasses of the above ServiceHandler classes
    based on a prefix and base. The prefix is set as the ``name`` attribute
    on the resulting type.

    e.g. ``service_factory("foo", ServicePoolHandler)`` returns a type
    called ``FooServicePoolHandler`` that inherits from ``ServicePoolHandler``,
    and ``FooServicePoolHandler.name`` is ``"foo"``.

    If prefix is falsy, return the base without modification.
    """
    if not prefix:
        return base
    name = prefix.title() + base.__name__
    cls = type(name, (base,), {"name": prefix})
    return cls


@pytest.fixture
def make_containers(request, container_factory, rabbit_config):
    def make(base, prefixes=("",)):
        """ Use ``service_factory`` to create a service type inheriting from
        ``base`` using the given prefixes, and start a container for that
        service.

        If a prefix is given multiple times, create multiple containers for
        that service type. If no prefixes are given, create a single container
        with a type that does not extend the base.

        Stops all started containers when the test ends.
        """
        services = {}
        for prefix in prefixes:
            key = (prefix, base)
            if key not in services:
                service = service_factory(prefix, base)
                services[key] = service
            service = services.get(key)
            ct = container_factory(service, rabbit_config)
            ct.start()
            request.addfinalizer(ct.stop)
    return make


def test_service_pooled_events(reset_rabbit, reset_state,
                               rabbit_manager, rabbit_config,
                               make_containers):
    vhost = rabbit_config['vhost']
    make_containers(ServicePoolHandler, ("foo", "foo", "bar"))

    # foo service pool queue should have two consumers
    foo_queue = rabbit_manager.get_queue(
        vhost, "evt-srcservice-eventtype--foo.handle")
    assert len(foo_queue['consumer_details']) == 2

    # bar service pool queue should have one consumer
    bar_queue = rabbit_manager.get_queue(
        vhost, "evt-srcservice-eventtype--bar.handle")
    assert len(bar_queue['consumer_details']) == 1

    exchange_name = "srcservice.events"
    rabbit_manager.publish(vhost, exchange_name, 'eventtype', 'msg')

    # a total of two events should be received
    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        while len(events) < 2:
            eventlet.sleep()

    # exactly one instance of each service should have been created
    # each should have received an event
    assert len(services['foo']) == 1
    assert isinstance(services['foo'][0], ServicePoolHandler)
    assert services['foo'][0].events == ["msg"]

    assert len(services['bar']) == 1
    assert isinstance(services['bar'][0], ServicePoolHandler)
    assert services['bar'][0].events == ["msg"]


def test_service_pooled_events_multiple_handlers(reset_rabbit, reset_state,
                                                 rabbit_manager, rabbit_config,
                                                 make_containers):
    vhost = rabbit_config['vhost']
    make_containers(DoubleServicePoolHandler, ("double",))

    # we should have two queues with a consumer each
    foo_queue_1 = rabbit_manager.get_queue(
        vhost, "evt-srcservice-eventtype--double.handle_1")
    assert len(foo_queue_1['consumer_details']) == 1

    foo_queue_2 = rabbit_manager.get_queue(
        vhost, "evt-srcservice-eventtype--double.handle_2")
    assert len(foo_queue_2['consumer_details']) == 1

    exchange_name = "srcservice.events"
    rabbit_manager.publish(vhost, exchange_name, 'eventtype', 'msg')

    # each handler (3 of them) of the two services should have received the evt
    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        while len(events) < 2:
            eventlet.sleep()

    # two worker instances would have been created to deal with the handling
    assert len(services['double']) == 2
    assert services['double'][0].events == ["msg"]
    assert services['double'][1].events == ["msg"]


def test_singleton_events(reset_rabbit, reset_state,
                          rabbit_manager, rabbit_config,
                          make_containers):

    vhost = rabbit_config['vhost']
    make_containers(SingletonHandler, ("foo", "foo", "bar"))

    # the singleton queue should have three consumers
    queue = rabbit_manager.get_queue(vhost, "evt-srcservice-eventtype")
    assert len(queue['consumer_details']) == 3

    exchange_name = "srcservice.events"
    rabbit_manager.publish(vhost, exchange_name, 'eventtype', 'msg')

    # exactly one event should have been received
    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        while len(events) < 1:
            eventlet.sleep()

    # one lucky handler should have received the event
    assert len(services) == 1
    lucky_service = next(services.iterkeys())
    assert len(services[lucky_service]) == 1
    assert isinstance(services[lucky_service][0], SingletonHandler)
    assert services[lucky_service][0].events == ["msg"]


def test_broadcast_events(reset_rabbit, reset_state,
                          rabbit_manager, rabbit_config,
                          make_containers):
    vhost = rabbit_config['vhost']
    make_containers(BroadcastHandler, ("foo", "foo", "bar"))

    # each broadcast queue should have one consumer
    queues = rabbit_manager.get_queues(vhost)
    queue_names = [queue['name'] for queue in queues
                   if queue['name'].startswith("evt-srcservice-eventtype-")]

    assert len(queue_names) == 3
    for name in queue_names:
        queue = rabbit_manager.get_queue(vhost, name)
        assert len(queue['consumer_details']) == 1

    exchange_name = "srcservice.events"
    rabbit_manager.publish(vhost, exchange_name, 'eventtype', 'msg')

    # a total of three events should be received
    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        while len(events) < 3:
            eventlet.sleep()

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


def test_requeue_on_error(reset_rabbit, reset_state,
                          rabbit_manager, rabbit_config,
                          make_containers):
    vhost = rabbit_config['vhost']
    make_containers(RequeueingHandler)

    # the queue should been created and have one consumer
    queue = rabbit_manager.get_queue(
        vhost, "evt-srcservice-eventtype--requeue.handle")
    assert len(queue['consumer_details']) == 1

    exchange_name = "srcservice.events"
    rabbit_manager.publish(vhost, exchange_name, 'eventtype', 'msg')

    # the event will be received multiple times as it gets requeued and then
    # consumed again
    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        while len(events) < 2:
            eventlet.sleep()

    # multiple instances of the service should have been instantiated
    assert len(services['requeue']) > 1

    # each instance should have received one event
    for service in services['requeue']:
        assert service.events == ["msg"]


def test_reliable_delivery(reset_rabbit, reset_state,
                           rabbit_manager, rabbit_config,
                           container_factory):
    """ Events sent to queues declared by ``reliable_delivery`` handlers
    should be received even if no service was listening when they were
    dispatched.
    """
    vhost = rabbit_config['vhost']

    container = container_factory(ServicePoolHandler, rabbit_config)
    container.start()

    # test queue created, with one consumer
    queue_name = "evt-srcservice-eventtype--srvname.handle"
    queue = rabbit_manager.get_queue(vhost, queue_name)
    assert len(queue['consumer_details']) == 1

    # publish an event
    exchange_name = "srcservice.events"
    rabbit_manager.publish(vhost, exchange_name, 'eventtype', 'msg_1')

    # wait for the event to be received
    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        while len(events) == 0:
            eventlet.sleep()
    assert events == ["msg_1"]

    # stop container, check queue still exists, without consumers
    container.stop()
    queues = rabbit_manager.get_queues(vhost)
    assert queue_name in [q['name'] for q in queues]
    queue = rabbit_manager.get_queue(vhost, queue_name)
    assert len(queue['consumer_details']) == 0

    # publish another event while nobody is listening
    rabbit_manager.publish(vhost, exchange_name, 'eventtype', 'msg_2')

    # verify the message gets queued
    messages = rabbit_manager.get_messages(vhost, queue_name, requeue=True)
    assert ['msg_2'] == [msg['payload'] for msg in messages]

    # start another container
    container = container_factory(ServicePoolHandler, rabbit_config)
    container.start()

    # wait for the service to collect the pending event
    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        while len(events) < 2:
            eventlet.sleep()
    assert len(events) == 2
    assert events == ["msg_1", "msg_2"]

    container.stop()


def test_unreliable_delivery(reset_rabbit, reset_state,
                             rabbit_manager, rabbit_config,
                             make_containers, container_factory):
    """ Events sent to queues declared by non- ``reliable_delivery`` handlers
    should be lost if no service was listening when they were dispatched.
    """
    vhost = rabbit_config['vhost']

    container = container_factory(UnreliableHandler, rabbit_config)
    container.start()

    # start a normal handler service so the auto-delete events exchange
    # for srcservice is not removed when we stop the UnreliableHandler
    make_containers(ServicePoolHandler)

    # test queue created, with one consumer
    queue_name = "evt-srcservice-eventtype--unreliable.handle"
    queue = rabbit_manager.get_queue(vhost, queue_name)
    assert len(queue['consumer_details']) == 1

    # publish an event
    exchange_name = "srcservice.events"
    rabbit_manager.publish(vhost, exchange_name, 'eventtype', 'msg_1')

    # wait for it to arrive in both services
    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        while len(events) < 2:
            eventlet.sleep()
    assert events == ["msg_1", "msg_1"]

    # test that the unreliable service received it
    assert len(services['unreliable']) == 1
    assert services['unreliable'][0].events == ["msg_1"]

    # stop container, test queue deleted
    container.stop()
    queues = rabbit_manager.get_queues(vhost)
    assert queue_name not in [q['name'] for q in queues]

    # publish a second event while nobody is listening
    rabbit_manager.publish(vhost, exchange_name, 'eventtype', 'msg_2')

    # start another container
    container = container_factory(UnreliableHandler, rabbit_config)
    container.start()

    # verify the queue is recreated, with one consumer
    queue = rabbit_manager.get_queue(vhost, queue_name)
    assert len(queue['consumer_details']) == 1

    # publish a third event
    rabbit_manager.publish(vhost, exchange_name, 'eventtype', 'msg_3')

    # wait for it to arrive in both services
    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        while len(events) < 5:
            eventlet.sleep()

    # verify that the "unreliable" handler didn't receive the message sent
    # while it wasn't running
    assert len(services['unreliable']) == 2
    assert services['unreliable'][0].events == ["msg_1"]
    assert services['unreliable'][1].events == ["msg_3"]

    container.stop()


def test_dispatch_to_rabbit(reset_rabbit, rabbit_manager, rabbit_config):

    vhost = rabbit_config['vhost']
    service = Mock()
    srv_ctx = ServiceContext("srcservice", None, None, config=rabbit_config)
    worker_ctx = WorkerContext(srv_ctx, service, None)

    dispatcher = EventDispatcher()
    dispatcher.name = "dispatch"

    dispatcher.start(srv_ctx)
    dispatcher.on_container_started(srv_ctx)

    # we should have an exchange but no queues
    exchanges = rabbit_manager.get_exchanges(vhost)
    queues = rabbit_manager.get_queues(vhost)
    assert "srcservice.events" in [exchange['name'] for exchange in exchanges]
    assert queues == []

    # manually add a queue to capture the events
    rabbit_manager.create_queue(vhost, "event-sink", auto_delete=True)
    rabbit_manager.create_binding(vhost, "srcservice.events", "event-sink",
                                  rt_key=ExampleEvent.type)

    dispatcher.call_setup(worker_ctx)
    service.dispatch(ExampleEvent("msg"))

    # test event receieved on manually added queue
    messages = rabbit_manager.get_messages(vhost, "event-sink")
    assert ['msg'] == [msg['payload'] for msg in messages]
