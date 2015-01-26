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


# TODO: keep?
    See amqp routing keys for `topic` exchanges for more info.

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
from logging import getLogger
import uuid

from kombu import Exchange, Queue

from nameko.constants import DEFAULT_RETRY_POLICY
from nameko.extensions import Dependency
from nameko.messaging import HeaderEncoder, PERSISTENT, Consumer

##
from kombu import Connection
from kombu.common import maybe_declare
from kombu.pools import producers, connections

from nameko.messaging import AMQP_URI_CONFIG_KEY
##


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


def event_dispatcher(nameko_config, **kwargs):
    """ Returns a function that dispatches events claiming to originate from
    a service called `container_service_name`.

    Enables services not hosted by nameko to dispatch events into a nameko
    cluster.
    """

    kwargs = kwargs.copy()
    retry = kwargs.pop('retry', True)
    retry_policy = kwargs.pop('retry_policy', DEFAULT_RETRY_POLICY)

    def dispatch(service_name, event_type, event_data):
        conn = Connection(nameko_config[AMQP_URI_CONFIG_KEY])

        exchange = get_event_exchange(service_name)

        with connections[conn].acquire(block=True) as connection:
            maybe_declare(exchange, connection)
            with producers[conn].acquire(block=True) as producer:
                msg = event_data
                routing_key = event_type
                producer.publish(
                    msg,
                    exchange=exchange,
                    routing_key=routing_key,
                    retry=retry,
                    retry_policy=retry_policy,
                    **kwargs)
    return dispatch


class EventHandlerConfigurationError(Exception):
    """ Raised when an event handler is misconfigured.
    """


class EventDispatcher(Dependency, HeaderEncoder):
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

    def setup(self):
        self.service_name = self.container.service_name
        self.config = self.container.config

    def acquire_injection(self, worker_ctx):
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
                 reliable_delivery=True, requeue_on_error=False):
        r"""
        Decorate a method as a handler of ``event_type`` events on the service
        called ``source_service``. ``event_type`` must be either a subclass of
        :class:`~.Event` with a class attribute ``type`` or a string matching
        the
        value of this attribute.
        ``handler_type`` determines the behaviour of the handler:

            - ``events.SERVICE_POOL``:

                Event handlers are pooled by service type and method,
                and one service instance from each pool receives the event. ::

                               .-[queue]- (service X handler-meth-1)
                              /
                    exchange o --[queue]- (service X handler-meth-2)
                              \
                               \          (service Y(instance 1) handler-meth)
                                \       /
                                 [queue]
                                        \
                                          (service Y(instance 2) handler-meth)


            - ``events.SINGLETON``:

                Events are received by only one registered handler, regardless
                of service type. If requeued on error, they may be handled
                by a different service instance. ::

                                           (service X handler-meth)
                                         /
                    exchange o -- [queue]
                                         \
                                           (service Y handler-meth)

            - ``events.BROADCAST``:

                Events will be received by every handler. Events are broadcast
                to every service instance, not just every service type
                - use wisely! ::

                                [queue]- (service X(instance 1) handler-meth)
                              /
                    exchange o - [queue]- (service X(instance 2) handler-meth)
                              \
                                [queue]- (service Y handler-meth)

        # TODO: this is defined by the Consumer actually...
        If `requeue_on_error` is true, handlers will return the event to the
        queue if an error occurs while handling it. Defaults to false.

        If `reliable_delivery` is true, events will be held in the queue
        until there is a handler to consume them. Defaults to true.

        Raises an ``EventHandlerConfigurationError`` if the ``handler_type``
        is set to ``BROADCAST`` and ``reliable_delivery`` is set to ``True``.
        """
        if reliable_delivery and handler_type is BROADCAST:
            raise EventHandlerConfigurationError(
                "Broadcast event handlers cannot be configured with reliable "
                "delivery.")

        self.source_service = source_service
        self.event_type = event_type
        self.handler_type = handler_type
        self.reliable_delivery = reliable_delivery

        super(EventHandler, self).__init__(
            queue=None, requeue_on_error=requeue_on_error)

    def setup(self):
        _log.debug('starting %s', self)

        # handler_type determines queue name
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
            queue_name = "evt-{}-{}--{}.{}-{}".format(self.source_service,
                                                      self.event_type,
                                                      service_name,
                                                      self.method_name,
                                                      uuid.uuid4().hex)

        exchange = get_event_exchange(self.source_service)

        # auto-delete queues if events are not reliably delivered
        auto_delete = not self.reliable_delivery
        self.queue = Queue(
            queue_name, exchange=exchange, routing_key=self.event_type,
            durable=True, auto_delete=auto_delete)

        super(EventHandler, self).setup()

event_handler = EventHandler.decorator
