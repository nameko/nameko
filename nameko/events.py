"""
Provides a high level interface to the core messaging module.

Events are special messages, which can be emitted by one service
and handled by other listenting services.

To emit an event, a service must define an `Event` class with a unique type
and dispatch an instance of it using the `EventDispatcher`.
Dispatching of events is done asynchronously. It is only guaranteed
that the event has been dispatched, not that it was received or handled by a
listener.

To listen to an event, a service must declare a handler using the
`handle_event` decorator, providing the target service and an event filter.


Standard Example:
TODO:

Singleton Example:
TODO:

"""

from __future__ import absolute_import
from logging import getLogger

from kombu import Exchange

from nameko.messaging import Publisher

log = getLogger(__name__)


class EventTypeMissing(Exception):
    """ Raised when an Event subclasses are defined without and event-type.
    """
    def __init__(self, name):
        msg = ("Event subclass '{}' cannot be created without "
               "a 'type' attribute.").format(name)

        super(EventTypeMissing, self).__init__(msg)


class EventTypeTooLong(Exception):
    """ Raised when event types are defined and longer than 255 bytes.
    """
    def __init__(self, event_type):
        msg = 'Event type "{}" too long. Should be < 255 bytes.'.format(
            event_type)
        super(EventTypeTooLong, self).__init__(msg)


class EventMeta(type):
    """ Ensures every Event subclass has it's own event-type defined,
    and that the type is less than 255 bytes in size.

    This is a limitation imposed by AMQP topic exchanges.
    """

    def __new__(mcs, name, bases, dct):
        try:
            event_type = dct['type']
        except KeyError:
            raise EventTypeMissing(name)
        else:
            if len(event_type) > 255:
                raise EventTypeTooLong(event_type)

        return super(EventMeta, mcs).__new__(mcs, name, bases, dct)


class Event(object):
    """ The base class for all events to be dispatched by an `EventDispatcher`.
    """
    __metaclass__ = EventMeta

    type = 'Event'
    """ The type of the event.

    Events can be name-spaced using the type property:
    e.g. type = 'spam.ham.eggs'

    See amqp routing keys for `topic` exchanges for more info.
    """

    def __init__(self, data):
        self.data = data


class EventDispatcher(Publisher):
    """ Provides an event dispatcher method via dependency injection.

    Events emitted will be dispatched via the service's events exchange,
    which automatically gets declared by the event dispatcher
    as a topic exchange.
    The name for the exchange will be `{service-name}.events`.

    Events, emitted via the dispatcher, will be serialized and published
    to the events exchange. The event's type attribute is used as the
    routing key, which can be used for filtering on the listener's side.

    The dispatcher will return as soon as the event message has been published.
    There is no guarantee that any service will receive the event, only
    that the event has been successfully dispatched.

    Example:

    class MyEvent(Event):
        type = 'spam.ham'


    class Spammer(object):
        dispatch_spam = EventDispatcher()

        def emit_spam(self):
            evt = MyEvent('ham and eggs')
            self.dispatch_spam(evt)

    """

    def __init__(self):
        super(EventDispatcher, self).__init__()

    def get_instance(self, container):
        #TODO: better accessor for service name required
        service_name = container.topic
        name = '{}.events'.format(service_name)

        # There is no reason to use anything but `topic` for the exchange type.
        # We could use `direct`, but recent updates to RabbitMQ have made topic
        # exchanges fast enough for our purposes.
        # To accomplish `fanout` behaviour one can just bind private queues
        # to the exchange and singleton behaviour by binding a named queue.
        exchange = Exchange(
            name, type='topic', durable=False, auto_delete=True)
        self.exchange = exchange

        publish = super(EventDispatcher, self).get_instance(container)

        def dispatch(evt):
            # TODO: serialization of the event, maybe take attrs or have a
            #       special serialize method?
            msg = evt.data
            routing_key = evt.type
            publish(msg, routing_key=routing_key)

        return dispatch
