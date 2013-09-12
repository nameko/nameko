import eventlet
import pytest

from nameko.events import (
    EventDispatcher, Event, EventTypeTooLong, EventTypeMissing,
    EventHandlerConfigurationError, event_handler, SINGLETON, BROADCAST)

EVENTS_TIMEOUT = 5


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
    @event_handler('spammer', 'spammed')
    def handle(self, evt):
        self.events.append(evt)


class DoubleHandler(Handler):
    @event_handler('spammer', 'spammed')
    def handle_1(self, evt):
        self.events.append(('handle_1', evt))

    @event_handler('spammer', 'spammed')
    def handle_2(self, evt):
        self.events.append(('handle_2', evt))


class ReliableSpamHandler(Handler):
    @event_handler('spammer', 'spammed')
    def handle(self, evt):
        self.events.append(evt)


class UnreliableSpamHandler(Handler):
    @event_handler('spammer', 'spammed', reliable_delivery=False)
    def handle(self, evt):
        self.events.append(evt)


class RequeueingSpamHandler(Handler):
    @event_handler('spammer', 'spammed', requeue_on_error=True)
    def handle(self, evt):
        self.events.append(evt)
        raise Exception('foobar')


class SingletonSpamHandler(Handler):
    @event_handler('spammer', 'spammed', handler_type=SINGLETON)
    def handle(self, evt):
        self.events.append(evt)


class BroadcastSpamHandler(Handler):
    # boradcast handlers may not have reliable_delivery set to True
    @event_handler('spammer', 'spammed', reliable_delivery=False,
                   handler_type=BROADCAST)
    def handle(self, evt):
        self.events.append(evt)


def test_emit_event_without_handlers(start_service):
    spammer = start_service(Spammer, 'spammer_nobody_listening')
    spammer.emit_event('ham and eggs')


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


def test_relyable_broadcast_config_error():
    with pytest.raises(EventHandlerConfigurationError):
        @event_handler(
            'foo', 'bar', reliable_delivery=True, handler_type=BROADCAST)
        def foo(self):
            pass


def test_service_pooled_events(start_service):
    handler_x = start_service(SpamHandler, 'spamhandler')
    handler_y = start_service(SpamHandler, 'spamhandler')
    handler_z = start_service(SpamHandler, 'special_spamhandler')

    spammer = start_service(Spammer, 'spammer')
    spammer.emit_event('ham and eggs')

    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        events = []
        while len(events) < 2:
            events = handler_x.events + handler_y.events + handler_z.events
            eventlet.sleep()

    # handler_z will receive the event
    assert handler_z.events == ['ham and eggs']

    # only one of handler_x or handler_y will receive the event
    assert handler_x.events + handler_y.events == ['ham and eggs']


def test_service_pooled_events_with_multiple_handlers(start_service):
    handler_x = start_service(DoubleHandler, 'spamhandler')
    handler_y = start_service(SpamHandler, 'special_spamhandler')

    spammer = start_service(Spammer, 'spammer')
    spammer.emit_event('ham and eggs')
    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        events = []
        while len(events) < 3:
            events = handler_x.events + handler_y.events
            eventlet.sleep()

    # handler_y will receive the event
    assert handler_y.events == ['ham and eggs']

    # both handlers of the DoubleHandler will receive the same event
    assert sorted(handler_x.events) == [
        ('handle_1', 'ham and eggs'),
        ('handle_2', 'ham and eggs'),
    ]


def test_singleton_events(start_service):
    handler_x = start_service(SingletonSpamHandler, 'spamhandler')
    handler_y = start_service(SingletonSpamHandler, 'spamhandler')
    handler_z = start_service(SingletonSpamHandler, 'special_spamhandler')

    spammer = start_service(Spammer, 'spammer')
    spammer.emit_event('ham and eggs')

    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        events = []
        while not events:
            events = handler_x.events + handler_y.events + handler_z.events
            eventlet.sleep()

    # only one handler should receive the event
    assert len(events) == 1


def test_broadcast_events(start_service):
    handler_x = start_service(BroadcastSpamHandler, 'spamhandler')
    handler_y = start_service(BroadcastSpamHandler, 'spamhandler')
    handler_z = start_service(BroadcastSpamHandler, 'special_spamhandler')

    spammer = start_service(Spammer, 'spammer')
    spammer.emit_event('ham and eggs')

    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        events = []
        while len(events) < 3:
            events = handler_x.events + handler_y.events + handler_z.events
            eventlet.sleep()

    # all handlers should receive the event
    assert len(events) == 3
    assert handler_x.events == ['ham and eggs']
    assert handler_y.events == ['ham and eggs']
    assert handler_z.events == ['ham and eggs']


def test_event_lost_without_listener(start_service, kill_services):
    handler = start_service(UnreliableSpamHandler, 'unreliable')

    spammer = start_service(Spammer, 'spammer')
    spammer.emit_event('ham')

    # we want to make sure that events have been received before
    # killing the services, that way we know we had an event queue
    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        events = handler.events
        while not events:
            eventlet.sleep()

    kill_services('unreliable')

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
    requeueing_handler = start_service(RequeueingSpamHandler, 'requeue')

    spammer = start_service(Spammer, 'spammer')
    spammer.emit_event('ham and eggs')

    # we want to wait until at least one event was received
    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        while len(requeueing_handler.events) < 1:
            eventlet.sleep()

    # the new service should pick up the event
    # we are pretending to be the same service with the same handler method
    handler = start_service(SpamHandler, 'requeue')

    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        while len(handler.events) < 1:
            eventlet.sleep()

    assert len(handler.events) == 1
