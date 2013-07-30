import pytest
import eventlet
from collections import defaultdict

from mock import Mock, patch

from nameko.events import (
    EventDispatcher, Event, EventTypeTooLong, EventTypeMissing,
    EventHandlerConfigurationError, event_handler, SINGLETON, BROADCAST,
    SERVICE_POOL, EventHandler)
from nameko.dependencies import DECORATOR_PROVIDERS_ATTR
from nameko.service import ServiceContext
from nameko.testing.utils import ANY_PARTIAL


EVENTS_TIMEOUT = 5


"""
class SpamEvent(Event):
    type = 'spammed'


class Spammer(object):

    dispatch = EventDispatcher()

    def emit_event(self, data):
        self.dispatch(SpamEvent(data))


class Handler(object):
    def __init__(self):
        self.events = []


class SpamHandler(Handler):
    # force reliable delivery off until we have test cleanup
    @event_handler('spammer', 'spammed', reliable_delivery=False)
    def handle(self, evt):
        self.events.append(evt)


class ReliableSpamHandler(Handler):
    @event_handler('spammer', 'spammed', reliable_delivery=True)
    def handle(self, evt):
        self.events.append(evt)


class RequeueingSpamHandler(Handler):
    @event_handler('spammer', 'spammed',
                   reliable_delivery=False, requeue_on_error=True)
    def handle(self, evt):
        self.events.append(evt)
        raise Exception('foobar')


class SingletonSpamHandler(Handler):
    # force reliable delivery off until we have test cleanup
    @event_handler('spammer', 'spammed', reliable_delivery=False,
                   handler_type=SINGLETON)
    def handle(self, evt):
        self.events.append(evt)


class BroadcastSpamHandler(Handler):
    # force reliable delivery off until we have test cleanup
    @event_handler('spammer', 'spammed', reliable_delivery=False,
                   handler_type=BROADCAST)
    def handle(self, evt):
        self.events.append(evt)
"""


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

    srv_ctx = ServiceContext('destservice', None, None)

    event_dispatcher.start(srv_ctx)
    assert event_dispatcher.exchange.name == "destservice.events"

    with patch('nameko.messaging.Publisher.__call__') as super_call:
        evt = Mock(type="eventtype")
        event_dispatcher.__call__(evt)

        super_call.assert_called_once_with(evt.data, routing_key=evt.type)


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
        event_handler.start(srv_ctx)
        assert event_handler.queue.name == \
            "evt-srcservice-eventtype-destservice"

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
    while len(services):
        services.popitem()
    while len(events):
        events.pop()


class Handler(object):

    def __init__(self):
        self.events = []
        services[self.name].append(self)


class ServicePoolHandler(Handler):

    @event_handler('srcservice', 'eventtype', handler_type=SERVICE_POOL)
    def handle(self, evt):
        self.events.append(evt)
        events.append(evt)


class SingletonHandler(Handler):

    @event_handler('srcservice', 'eventtype', handler_type=SINGLETON)
    def handle(self, evt):
        self.events.append(evt)
        events.append(evt)


class BroadcastHandler(Handler):

    @event_handler('srcservice', 'eventtype', handler_type=BROADCAST,
                   reliable_delivery=False)
    def handle(self, evt):
        self.events.append(evt)
        events.append(evt)


class BrokenHandler(Handler):

    @event_handler('srcservice', 'eventtype')
    def handle(self, evt):
        raise Exception("Error")


FooServicePoolHandler = type("FooServicePoolHandler",
                             (ServicePoolHandler,), {'name': 'foo'})

BarServicePoolHandler = type("BarServicePoolHandler",
                             (ServicePoolHandler,), {'name': 'bar'})

FooSingletonHandler = type("FooSingletonHandler",
                           (SingletonHandler,), {'name': 'foo'})

BarSingletonHandler = type("BarSingletonHandler",
                           (SingletonHandler,), {'name': 'bar'})

FooBroadcastHandler = type("FooBroadcastHandler",
                           (BroadcastHandler,), {'name': 'foo'})

BarBroadcastHandler = type("BarBroadcastHandler",
                           (BroadcastHandler,), {'name': 'bar'})


def test_service_pooled_events(reset_rabbit, rabbit_manager, rabbit_config,
                               container_factory, reset_state):
    vhost = rabbit_config['vhost']

    container_x = container_factory(FooServicePoolHandler, rabbit_config)
    container_y = container_factory(FooServicePoolHandler, rabbit_config)
    container_z = container_factory(BarServicePoolHandler, rabbit_config)
    container_x.start()
    container_y.start()
    container_z.start()

    # foo service pool queue should have two consumers
    foo_queue = rabbit_manager.get_queue(vhost, "evt-srcservice-eventtype-foo")
    assert len(foo_queue['consumer_details']) == 2

    # bar service pool queue should have one consumer
    bar_queue = rabbit_manager.get_queue(vhost, "evt-srcservice-eventtype-bar")
    assert len(bar_queue['consumer_details']) == 1

    exchange_name = "srcservice.events"
    rabbit_manager.publish(vhost, exchange_name, 'eventtype', 'msg')

    # a total of two events should be received
    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        while len(events) < 2:
            eventlet.sleep()

    # exactly one instance of each service should have been created
    assert len(services['foo']) == 1
    assert isinstance(services['foo'][0], FooServicePoolHandler)
    assert services['foo'][0].events == ["msg"]

    assert len(services['bar']) == 1
    assert isinstance(services['bar'][0], BarServicePoolHandler)
    assert services['bar'][0].events == ["msg"]

    container_x.stop()
    container_y.stop()
    container_z.stop()


def test_singleton_events(reset_rabbit, rabbit_manager, rabbit_config,
                          container_factory, reset_state):
    vhost = rabbit_config['vhost']

    container_x = container_factory(FooSingletonHandler, rabbit_config)
    container_y = container_factory(FooSingletonHandler, rabbit_config)
    container_z = container_factory(BarSingletonHandler, rabbit_config)
    container_x.start()
    container_y.start()
    container_z.start()

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

    container_x.stop()
    container_y.stop()
    container_z.stop()


def test_broadcast_events(reset_rabbit, rabbit_manager, rabbit_config,
                          container_factory, reset_state):
    vhost = rabbit_config['vhost']

    container_x = container_factory(FooBroadcastHandler, rabbit_config)
    container_y = container_factory(FooBroadcastHandler, rabbit_config)
    container_z = container_factory(BarBroadcastHandler, rabbit_config)
    container_x.start()
    container_y.start()
    container_z.start()

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
    assert isinstance(services['foo'][0], FooBroadcastHandler)
    assert isinstance(services['foo'][1], FooBroadcastHandler)

    # and they both should have received the event
    assert services['foo'][0].events == ["msg"]
    assert services['foo'][1].events == ["msg"]

    # the other was a "bar" handler
    assert len(services['bar']) == 1
    assert isinstance(services['bar'][0], BarBroadcastHandler)

    # and it too should have received the event
    assert services['bar'][0].events == ["msg"]

"""
def test_emit_event_without_handlers(start_service):
    spammer = start_service(Spammer, 'spammer_nobody_listening')
    spammer.emit_event('ham and eggs')


def test_event_lost_without_listener(start_service, kill_services):
    handler = start_service(SpamHandler, 'spamhandler')

    spammer = start_service(Spammer, 'spammer')
    spammer.emit_event('ham')

    # we want to make sure that events have been received before
    # killing the services, that way we know we had an event queue
    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        events = handler.events
        while not events:
            eventlet.sleep()

    kill_services('spamhandler')

    # we don't have any handler listening anymore, so the event should vanish
    spammer.emit_event('lost ham')

    handler = start_service(SpamHandler, 'spamhandler')
    # we now have a service listening and should receive events again
    spammer.emit_event('eggs')

    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        events = handler.events
        while not events:
            eventlet.sleep()

    assert events == ['eggs']


def test_event_not_lost_with_reliable_delivery(start_service, kill_services):
    spammer = start_service(Spammer, 'spammer')
    # this event should vanish, as there is no ReliableSpamHandler running yet
    spammer.emit_event('lost')

    # this service can die, as it's event queue will survive
    handler = start_service(ReliableSpamHandler, 'spamhandler')
    kill_services('spamhandler')
    eventlet.sleep()

    # since we have a reliable event queue, event s should not get lost
    spammer.emit_event('ham')

    handler = start_service(ReliableSpamHandler, 'spamhandler')
    spammer.emit_event('eggs')

    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        events = handler.events
        while len(events) < 2:
            eventlet.sleep()

    assert events == ['ham', 'eggs']


def test_requeue_event_on_error(start_service):
    requeueing_handler = start_service(RequeueingSpamHandler, 'spamhandler')

    spammer = start_service(Spammer, 'spammer')
    spammer.emit_event('ham and eggs')

    # we want to wait until at least one event was received
    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        while len(requeueing_handler.events) < 1:
            eventlet.sleep()

    # the new service should pick up the event
    handler = start_service(SpamHandler, 'spamhandler')

    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        while len(handler.events) < 1:
            eventlet.sleep()

    assert len(handler.events) == 1
"""
