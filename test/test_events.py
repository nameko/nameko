import eventlet
import pytest

from nameko.events import (
    EventDispatcher, Event, EventTypeTooLong, EventTypeMissing, event_handler,
    SINGLETON, BROADCAST)

EVENTS_TIMEOUT = 5


class SpamEvent(Event):
    type = 'spammed'


class Spammer(object):

    dispatch = EventDispatcher()

    def emit_event(self, data):
        self.dispatch(SpamEvent(data))


class SpamHandler(object):
    events = []

    # force reliable delivery off until we havve test cleanup
    @event_handler('spammer', 'spammed', reliable_delivery=False)
    def handle(self, evt):
        self.events.append(evt)


class SingletonSpamHandler(object):
    events = []

    # force reliable delivery off until we havve test cleanup
    @event_handler('spammer', 'spammed', reliable_delivery=False,
                   handler_type=SINGLETON)
    def handle(self, evt):
        self.events.append(evt)


class BroadcastSpamHandler(object):
    events = []

    # force reliable delivery off until we havve test cleanup
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


def test_service_pooled_events(start_service):
    handler_x = start_service(SpamHandler, 'spamhandler')
    handler_y = start_service(SpamHandler, 'spamhandler')
    handler_z = start_service(SpamHandler, 'special_spamhandler')

    spammer = start_service(Spammer, 'spammer')
    spammer.emit_event('ham and eggs')

    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        while not handler_z.events:
            eventlet.sleep()

    # handler_z will receive the event
    assert handler_z.events == ['ham and eggs']

    # only one of handler_x or handler_y will receive the event
    if handler_x.events:
        assert handler_x.events == ['ham and eggs']
        assert handler_y.events == []
    else:
        assert handler_x.events == []
        assert handler_y.events == ['ham and eggs']


def test_singleton_events(start_service):
    handler_x = start_service(SingletonSpamHandler, 'spamhandler')
    handler_y = start_service(SingletonSpamHandler, 'spamhandler')
    handler_z = start_service(SingletonSpamHandler, 'special_spamhandler')

    spammer = start_service(Spammer, 'spammer')
    spammer.emit_event('ham and eggs')

    with eventlet.timeout.Timeout(EVENTS_TIMEOUT):
        events = handler_x.events + handler_y.events + handler_z.events
        while not events:
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
        events = handler_x.events + handler_y.events + handler_z.events
        while not events:
            eventlet.sleep()

    # all handlers should receive the event
    assert len(events) == 3
    assert handler_x.events == ['ham and eggs']
    assert handler_y.events == ['ham and eggs']
    assert handler_z.events == ['ham and eggs']


# TODO: tests for reliable delivery, requeue on error
