"""
Provides a high level interface to the core messaging module.

Events are special messages, which can be emitted by one service
and handled by other listening services.

To emit an event, a service must define an :class:`Event` class with a unique
type and dispatch an instance of it using an injection acquired from an
instance of :class:`EventDispatcher`.

Events are dispatched asynchronously. It is only guaranteed that the event has
been dispatched, not that it was received or handled by a listener.

To listen to an event, a service must declare a handler using the
:func:`handle_event` entrypoint, providing the target service and an event type
filter.

Example::

    class MyEvent(Event):
        type = "myevent"

    @handle_event("foo_service", "myevent")
    def bar(evt):
        pass

"""
from __future__ import absolute_import
from logging import getLogger
import uuid

from kombu import Exchange, Queue

from nameko.messaging import PublishProvider, PERSISTENT, ConsumeProvider
from nameko.dependencies import entrypoint, injection, DependencyFactory


SERVICE_POOL = "service_pool"
SINGLETON = "singleton"
BROADCAST = "broadcast"

_log = getLogger(__name__)


def get_event_exchange(service_name):
    """ Get an exchange for ``service_name`` events.
    """
    exchange_name = "{}.events".format(service_name)
    exchange = Exchange(
        exchange_name, type='topic', durable=True, auto_delete=True,
        delivery_mode=PERSISTENT)

    return exchange


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


class EventHandlerConfigurationError(Exception):
    """ Raised when an event handler is misconfigured.
    """


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
    e.g.: ``type = 'spam.ham.eggs'``

    See amqp routing keys for `topic` exchanges for more info.
    """

    def __init__(self, data):
        self.data = data


class EventDispatcher(PublishProvider):
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

    Example::

        class MyEvent(Event):
            type = 'spam.ham'


        class Spammer(object):
            dispatch_spam = EventDispatcher()

            def emit_spam(self):
                evt = MyEvent('ham and eggs')
                self.dispatch_spam(evt)

    """
    def prepare(self):
        service_name = self.container.service_name
        self.exchange = get_event_exchange(service_name)
        super(EventDispatcher, self).prepare()

    def acquire_injection(self, worker_ctx):
        """ Inject a dispatch method onto the service instance
        """
        def dispatch(evt):
            exchange = self.exchange

            msg = evt.data
            routing_key = evt.type

            with self.get_producer() as producer:

                headers = self.get_message_headers(worker_ctx)
                producer.publish(msg, exchange=exchange, headers=headers,
                                 routing_key=routing_key)

        return dispatch


@injection
def event_dispatcher():
    return DependencyFactory(EventDispatcher)


class EventHandler(ConsumeProvider):

    def __init__(self, service_name, event_type, handler_type,
                 reliable_delivery, requeue_on_error):

        self.service_name = service_name
        self.event_type = event_type
        self.handler_type = handler_type
        self.reliable_delivery = reliable_delivery

        super(EventHandler, self).__init__(
            queue=None, requeue_on_error=requeue_on_error)

    def prepare(self):
        _log.debug('starting handler for %s', self.container)

        # handler_type determines queue name
        service_name = self.container.service_name
        if self.handler_type is SERVICE_POOL:
            queue_name = "evt-{}-{}--{}.{}".format(self.service_name,
                                                   self.event_type,
                                                   service_name,
                                                   self.name)
        elif self.handler_type is SINGLETON:
            queue_name = "evt-{}-{}".format(self.service_name,
                                            self.event_type)
        elif self.handler_type is BROADCAST:
            queue_name = "evt-{}-{}--{}.{}-{}".format(self.service_name,
                                                      self.event_type,
                                                      service_name,
                                                      self.name,
                                                      uuid.uuid4().hex)

        exchange = get_event_exchange(self.service_name)

        # auto-delete queues if events are not reliably delivered
        auto_delete = not self.reliable_delivery
        self.queue = Queue(
            queue_name, exchange=exchange, routing_key=self.event_type,
            durable=True, auto_delete=auto_delete)

        super(EventHandler, self).prepare()


@entrypoint
def event_handler(service_name, event_type, handler_type=SERVICE_POOL,
                  reliable_delivery=True, requeue_on_error=False,
                  event_handler_cls=EventHandler):
    r"""
    Decorate a method as a handler of ``event_type`` events on the service
    called ``service_name``. ``event_type`` must be either a subclass of
    :class:`~.Event` with a class attribute ``type`` or a string matching the
    value of this attribute.
    ``handler_type`` determines the behaviour of the handler:

        - ``events.SERVICE_POOL``:

            Event handlers will be pooled by service type and handler-method
            and one from each pool will receive the event. ::

                           .-[queue]- (service X handler-method-1)
                          /
                exchange o --[queue]- (service X handler-method-2)
                          \
                           \          (service Y(instance 1) hanlder-method)
                            \       /
                             [queue]
                                    \
                                      (service Y(instance 2) handler-method)


        - ``events.SINGLETON``:

            Events will be received by only one registered handler.
            If requeued on error, they may be given to a different
            handler. ::

                                       (service X handler-method)
                                     /
                exchange o -- [queue]
                                     \
                                       (service Y handler-method)

        - ``events.BROADCAST``:
            Events will be received by every handler. This  will broadcast
            to every service instance, not just every service type
            - use wisely! ::

                            [queue]- (service X(instance 1) handler-method)
                          /
                exchange o - [queue]- (service X(instance 2) handler-method)
                          \
                            [queue]- (service Y handler-method)

    If ``requeue_on_error``, handlers will return the event to the queue if an
    error occurs while handling it. Defaults to False.

    If ``reliable_delivery``, events will be kept in the queue until there is
    a handler to consume them. Defaults to ``True``.

    ``event_handler_cls`` may be specified to use a different EventHandler
        (sub)class for custom behaviour.

    Raises an ``EventHandlerConfigurationError`` if the ``handler_type``
    is set to ``BROADCAST`` and ``reliable_delivery`` is set to ``True``.
    """

    if reliable_delivery and handler_type is BROADCAST:
        raise EventHandlerConfigurationError(
            "Broadcast event handlers cannot be configured with reliable "
            "delivery.")

    if isinstance(event_type, type) and issubclass(event_type, Event):
        event_type = event_type.type
    elif not isinstance(event_type, basestring):
        raise TypeError(
            'event_type must be either a nameko.events.Event subclass or a '
            'string a string matching the Event.type value. '
            'Got {}'.format(type(event_type).__name__))

    return DependencyFactory(event_handler_cls, service_name, event_type,
                             handler_type, reliable_delivery, requeue_on_error)
