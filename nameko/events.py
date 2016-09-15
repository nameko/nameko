"""
Provides a high level interface to the core messaging module.

Events are special messages, which can be emitted by one service
and handled by other listening services.

An event consists of an identifier and some data and is dispatched using an
injection acquired from an instance of :class:`EventDispatcher`.

Events are dispatched asynchronously. It is only guaranteed that the event has
been dispatched, not that it was received or handled by a listener.

To listen to an event, a service must declare a handler using the
:func:`handle_event` entrypoint, providing the target service and an event type
filter.

Example::

    # service A
    def edit_foo(self, id):
        # ...
        self.dispatch('foo_updated', {'id': id})

    # service B

    @handle_event('service_a', 'foo_updated')
    def bar(event_data):
        pass

"""
from __future__ import absolute_import

import uuid
from logging import getLogger

from kombu import Queue
from nameko.messaging import Consumer, Publisher
from nameko.standalone.events import event_dispatcher, get_event_exchange

SERVICE_POOL = "service_pool"
SINGLETON = "singleton"
BROADCAST = "broadcast"

_log = getLogger(__name__)


class EventHandlerConfigurationError(Exception):
    """ Raised when an event handler is misconfigured.
    """


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

    Example::

        class Spammer(object):
            dispatch_spam = EventDispatcher()

            def emit_spam(self):
                evt_data = 'ham and eggs'
                self.dispatch_spam('spam.ham', evt_data)

    """
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        super(EventDispatcher, self).__init__()

    def setup(self):
        self.service_name = self.container.service_name
        self.config = self.container.config
        self.exchange = get_event_exchange(self.service_name)
        super(EventDispatcher, self).setup()

    def get_dependency(self, worker_ctx):
        """ Inject a dispatch method onto the service instance
        """
        headers = self.get_message_headers(worker_ctx)
        kwargs = self.kwargs
        dispatcher = event_dispatcher(self.config, headers=headers, **kwargs)

        def dispatch(event_type, event_data):
            dispatcher(self.service_name, event_type, event_data)
        return dispatch


class EventHandler(Consumer):

    def __init__(self, source_service, event_type, handler_type=SERVICE_POOL,
                 reliable_delivery=True, requeue_on_error=False,
                 sensitive_variables=()):
        r"""
        Decorate a method as a handler of ``event_type`` events on the service
        called ``source_service``.
        :Parameters:
            source_service : str
                Name of the service that dispatches the event
            event_type : str
                Type of the event to handle
            handler_type : str
                Determines the behaviour of the handler in a cluster:
                - ``events.SERVICE_POOL``:
                    Event handlers are pooled by service type and method,
                    and one service instance from each pool receives the
                    event. ::
                                   .-[queue]- (service X handler-meth-1)
                                  /
                        exchange o --[queue]- (service X handler-meth-2)
                                  \
                                   \          (service Y(inst. 1) handler-meth)
                                    \       /
                                     [queue]
                                            \
                                              (service Y(inst. 2) handler-meth)
                - ``events.SINGLETON``:
                    Events are received by only one registered handler,
                    regardless of service type. If requeued on error, they may
                    be handled by a different service instance. ::
                                               (service X handler-meth)
                                             /
                        exchange o -- [queue]
                                             \
                                               (service Y handler-meth)
                - ``events.BROADCAST``:
                    Events will be received by every handler. Events are
                    broadcast to every service instance, not just every service
                    type. Instances are differentiated using
                    :attr:`EventHandler.broadcast_identifier`. ::
                                    [queue]- (service X(inst. 1) handler-meth)
                                  /
                        exchange o - [queue]- (service X(inst. 2) handler-meth)
                                  \
                                    [queue]- (service Y handler-meth)

            requeue_on_error : bool  # TODO: defined by Consumer actually..
                If true, handlers will return the event to the queue if an
                error occurs while handling it. Defaults to False.
            reliable_delivery : bool
                If true, events will be held in the queue until there is a
                handler to consume them. Defaults to True.
            sensitive_variables : string or tuple of strings
                Mark an argument or part of an argument as sensitive. Saved
                on the entrypoint instance as
                ``entrypoint.sensitive_variables`` for later inspection by
                other extensions, for example a logging system.
                :seealso: :func:`nameko.utils.get_redacted_args`
        """
        self.source_service = source_service
        self.event_type = event_type
        self.handler_type = handler_type
        self.reliable_delivery = reliable_delivery
        self.sensitive_variables = sensitive_variables

        super(EventHandler, self).__init__(
            queue=None, requeue_on_error=requeue_on_error)

    @property
    def broadcast_identifier(self):
        """ A unique string to identify a service instance for `BROADCAST`
        type handlers.

        The `broadcast_identifier` is appended to the queue name when the
        `BROADCAST` handler type is used. It must uniquely identify service
        instances that receive broadcasts.

        The default `broadcast_identifier` is a uuid that is set when the
        service starts. It will change when the service restarts, meaning
        that any unconsumed messages that were broadcast to the 'old' service
        instance will not be received by the 'new' one. ::

            @property
            def broadcast_identifier(self):
                # use a uuid as the identifier.
                # the identifier will change when the service restarts and
                # any unconsumed messages will be lost
                return uuid.uuid4().hex

        The default behaviour is therefore incompatible with reliable delivery.

        An alternative `broadcast_identifier` that would survive service
        restarts is ::

            @property
            def broadcast_identifier(self):
                # use the machine hostname as the identifier.
                # this assumes that only one instance of a service runs on
                # any given machine
                return socket.gethostname()

        If neither of these approaches are appropriate, you could read the
        value out of a configuration file ::

            @property
            def broadcast_identifier(self):
                return self.config['SERVICE_IDENTIFIER']  # or similar

        Broadcast queues are exclusive to ensure that `broadcast_identifier`
        values are unique.

        Because this method is a descriptor, it will be called during
        container creation, regardless of the configured `handler_type`.
        See :class:`nameko.extensions.Extension` for more details.
        """
        if self.handler_type is not BROADCAST:
            return None

        if self.reliable_delivery:
            raise EventHandlerConfigurationError(
                "You are using the default broadcast identifier "
                "which is not compatible with reliable delivery. See "
                ":meth:`nameko.events.EventHandler.broadcast_identifier` "
                "for details.")

        return uuid.uuid4().hex

    def setup(self):
        _log.debug('starting %s', self)

        # handler_type determines queue name and exclusive flag
        exclusive = False
        service_name = self.container.service_name
        if self.handler_type is SERVICE_POOL:
            queue_name = "evt-{}-{}--{}.{}".format(self.source_service,
                                                   self.event_type,
                                                   service_name,
                                                   self.method_name)
        elif self.handler_type is SINGLETON:
            queue_name = "evt-{}-{}".format(self.source_service,
                                            self.event_type)
        elif self.handler_type is BROADCAST:
            broadcast_identifier = self.broadcast_identifier
            queue_name = "evt-{}-{}--{}.{}-{}".format(self.source_service,
                                                      self.event_type,
                                                      service_name,
                                                      self.method_name,
                                                      broadcast_identifier)

        exchange = get_event_exchange(self.source_service)

        # queues for handlers without reliable delivery should be marked as
        # auto-delete so they're removed when the consumer disconnects
        auto_delete = self.reliable_delivery is False

        # queues for broadcast handlers are exclusive (meaning that only one
        # consumer may be connected) except when reliable delivery is enabled,
        # because exclusive queues are always removed when the consumer
        # disconnects, regardless of the value of auto_delete
        exclusive = self.handler_type is BROADCAST
        if self.reliable_delivery:
            exclusive = False

        self.queue = Queue(
            queue_name, exchange=exchange, routing_key=self.event_type,
            durable=True, auto_delete=auto_delete, exclusive=exclusive)

        super(EventHandler, self).setup()

event_handler = EventHandler.decorator
