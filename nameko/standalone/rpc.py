from __future__ import absolute_import

import logging
import socket
import uuid

from amqp.exceptions import ConnectionError
from kombu import Connection
from kombu.common import maybe_declare
from kombu.messaging import Consumer, Queue
from kombu.mixins import ConsumerMixin

from nameko.amqp import verify_amqp_uri
from nameko.amqp.publish import Publisher
from nameko.constants import (
    AMQP_URI_CONFIG_KEY, DEFAULT_SERIALIZER, SERIALIZER_CONFIG_KEY,
    HEARTBEAT_CONFIG_KEY, DEFAULT_HEARTBEAT
)
from nameko.exceptions import RpcTimeout
from nameko.messaging import encode_to_headers
from nameko.rpc import (
    ServiceProxy, get_rpc_exchange,
    RPC_REPLY_QUEUE_TEMPLATE, RPC_REPLY_QUEUE_TTL
)


_logger = logging.getLogger(__name__)


# class ConsumeEvent(object):
#     """ Event for the RPC consumer with the same interface as eventlet.Event.
#     """
#     exception = None

#     def __init__(self, queue_consumer, correlation_id):
#         self.correlation_id = correlation_id
#         self.queue_consumer = queue_consumer  # do we need to pass this in?

#     def send(self, body):
#         self.body = body

#     def send_exception(self, exc):
#         self.exception = exc

#     def wait(self):
#         """ Makes a blocking call to its queue_consumer until the message
#         with the given correlation_id has been processed.

#         By the time the blocking call exits, self.send() will have been called
#         with the body of the received message
#         (see :meth:`~nameko.rpc.ReplyListener.handle_message`).

#         Exceptions are raised directly.
#         """
#         # disconnected before starting to wait
#         if self.exception:
#             raise self.exception

#         if self.queue_consumer.stopped:
#             raise RuntimeError(
#                 "This consumer has been stopped, and can no longer be used"
#             )
#         if self.queue_consumer.connection.connected is False:
#             # we can't just reconnect here. the consumer (and its exclusive,
#             # auto-delete reply queue) must be re-established _before_ sending
#             # any request, otherwise the reply queue may not exist when the
#             # response is published.
#             raise RuntimeError(
#                 "This consumer has been disconnected, and can no longer "
#                 "be used"
#             )

#         try:
#             self.queue_consumer.get_message(self.correlation_id)
#         except socket.error as exc:
#             self.exception = exc

#         # disconnected while waiting
#         if self.exception:
#             raise self.exception
#         return self.body


# class PollingQueueConsumer(object):
#     """ Implements a minimum interface of the
#     :class:`~messaging.QueueConsumer`. Instead of processing messages in a
#     separate thread it provides a polling method to block until a message with
#     the same correlation ID of the RPC-proxy call arrives.
#     """
#     consumer = None

#     def __init__(self, timeout=None):
#         self.stopped = True
#         self.timeout = timeout
#         self.replies = {}

#     def _setup_consumer(self):
#         if self.consumer is not None:
#             try:
#                 self.consumer.cancel()
#             except (socket.error, IOError):  # pragma: no cover
#                 # On some systems (e.g. os x) we need to explicitly cancel the
#                 # consumer here. However, e.g. on ubuntu 14.04, the
#                 # disconnection has already closed the socket. We try to
#                 # cancel, and ignore any socket errors.
#                 # If the socket has been closed, an IOError is raised, ignore
#                 # it and assume the consumer is already cancelled.
#                 pass

#         channel = self.connection.channel()
#         # queue.bind returns a bound copy
#         self.queue = self.queue.bind(channel)
#         maybe_declare(self.queue, channel)
#         consumer = Consumer(
#             channel, queues=[self.queue], accept=self.accept, no_ack=False)
#         consumer.callbacks = [self.on_message]
#         consumer.consume()
#         self.consumer = consumer

#     def register_provider(self, provider):
#         self.provider = provider

#         self.serializer = provider.container.config.get(
#             SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER)
#         self.accept = [self.serializer]

#         amqp_uri = provider.container.config[AMQP_URI_CONFIG_KEY]
#         verify_amqp_uri(amqp_uri)
#         self.connection = Connection(amqp_uri)

#         self.queue = provider.queue
#         self._setup_consumer()
#         self.stopped = False

#     def unregister_provider(self, provider):
#         self.connection.close()
#         self.stopped = True

#     def ack_message(self, msg):
#         msg.ack()

#     def on_message(self, body, message):
#         msg_correlation_id = message.properties.get('correlation_id')
#         if msg_correlation_id not in self.provider._reply_events:
#             _logger.debug(
#                 "Unknown correlation id: %s", msg_correlation_id)

#         self.replies[msg_correlation_id] = (body, message)

#     def get_message(self, correlation_id):

#         try:
#             while correlation_id not in self.replies:
#                 self.consumer.connection.drain_events(
#                     timeout=self.timeout
#                 )

#             body, message = self.replies.pop(correlation_id)
#             self.provider.handle_message(body, message)

#         except socket.timeout:
#             # TODO: this conflates an rpc timeout with a socket read timeout.
#             # a better rpc proxy implementation would recover from a socket
#             # timeout if the rpc timeout had not yet been reached
#             timeout_error = RpcTimeout(self.timeout)
#             event = self.provider._reply_events.pop(correlation_id)
#             event.send_exception(timeout_error)

#             # timeout is implemented using socket timeout, so when it
#             # fires the connection is closed and must be re-established
#             self._setup_consumer()

#         except (IOError, ConnectionError) as exc:
#             # in case this was a temporary error, attempt to reconnect
#             # and try again. if we fail to reconnect, the error will bubble
#             self._setup_consumer()
#             self.get_message(correlation_id)

#         except KeyboardInterrupt as exc:
#             event = self.provider._reply_events.pop(correlation_id)
#             event.send_exception(exc)
#             # exception may have killed the connection
#             self._setup_consumer()


class ReplyEvent(object):
    """ Same interface as eventlet.Event but actually fetches the message

    Pretty pointless since it relies on the same ReplyListener that
    generates it. Only exists because MethodProxy expects this interface.
    """

    def __init__(self, reply_listener, correlation_id):
        self.reply_listener = reply_listener
        self.correlation_id = correlation_id

    def wait(self):
        return self.reply_listener.get_reply(self.correlation_id)


class ReplyListener(ConsumerMixin):

    def __init__(self, config, timeout=None):
        self.config = config
        self.timeout = timeout
        self.uuid = str(uuid.uuid4())

        queue_name = RPC_REPLY_QUEUE_TEMPLATE.format(
            "standalone_rpc_proxy", self.uuid
        )
        exchange = get_rpc_exchange(config)

        self.queue = Queue(
            queue_name,
            exchange=exchange,
            routing_key=self.routing_key,
            queue_arguments={
                'x-expires': RPC_REPLY_QUEUE_TTL
            }
        )
        self.pending = {}
        verify_amqp_uri(self.amqp_uri)

    @property
    def amqp_uri(self):
        return self.config[AMQP_URI_CONFIG_KEY]

    @property
    def accept(self):
        return self.config.get(
            SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER
        )

    @property
    def routing_key(self):
        # needed by methodproxy
        return self.uuid

    def get_reply_event(self, correlation_id):
        # needed by methodproxy
        self.pending[correlation_id] = None
        return ReplyEvent(self, correlation_id)

    def start(self):
        # TODO: do we need to do this here?
        self.should_stop = False
        with self.connection as conn:
            maybe_declare(self.queue, conn)

    def stop(self):
        self.should_stop = True

    @property
    def connection(self):
        """ Provide the connection parameters for kombu's ConsumerMixin.

        The `Connection` object is a declaration of connection parameters
        that is lazily evaluated. It doesn't represent an established
        connection to the broker at this point.
        """
        heartbeat = self.config.get(
            HEARTBEAT_CONFIG_KEY, DEFAULT_HEARTBEAT
        )
        return Connection(self.amqp_uri, heartbeat=heartbeat)

    def get_consumers(self, consumer_cls, channel):
        """ Kombu callback to set up consumers.

        Called after any (re)connection to the broker.
        """
        consumer = consumer_cls(
            queues=[self.queue],
            callbacks=[self.handle_message],
            accept=[self.accept]
        )
        return [consumer]

    def get_reply(self, correlation_id):
        # return error if correlation_id not pending? (new feature)
        if self.should_stop:
            raise RuntimeError("Stopped and can no longer be used")

        while not self.pending.get(correlation_id):
            next(self.consume(timout=self.timeout))
        return self.pending.pop(correlation_id)

    def handle_message(self, body, message):
        message.ack()

        correlation_id = message.properties.get('correlation_id')
        if correlation_id not in self.pending:
            _logger.debug("Unknown correlation id: %s", correlation_id)
            return

        self.pending[correlation_id] = body


class StandaloneProxyBase(object):

    _proxy = None
    publisher_cls = Publisher

    def __init__(self, config, context_data=None, timeout=None):
        self.config = config
        self.reply_listener = ReplyListener(config, timeout=timeout)

        exchange = get_rpc_exchange(config)
        extra_headers = encode_to_headers(context_data) if context_data else {}
        # call id stack? we don't know the target method name at this point

        serializer = config.get(SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER)
        # options?

        publisher = Publisher(
            self.amqp_uri,
            serializer=serializer
        )
        import functools
        # can we get a nicer api than passing in a publish function? e.g. an "invoke" func?
        self._publish = functools.partial(publisher.publish, exchange=exchange, extra_headers=extra_headers)

    @property
    def amqp_uri(self):
        return self.config[AMQP_URI_CONFIG_KEY]

    def __enter__(self):
        return self.start()

    def __exit__(self, tpe, value, traceback):
        self.stop()

    def start(self):
        # start listening for replies (declare queue, register with queue consumer)
        self.reply_listener.start()
        return self._proxy  # set in subclass __init__

    def stop(self):
        # stop listening for replies (unregister from queue consumer, "stops" qc)
        self.reply_listener.stop()


class ServiceRpcProxy(StandaloneProxyBase):
    """
    A single-threaded RPC proxy to a named service. Method calls on the
    proxy are converted into RPC calls to the service, with responses
    returned directly.

    Enables services not hosted by nameko to make RPC requests to a nameko
    cluster. It is commonly used as a context manager but may also be manually
    started and stopped.

    *Usage*

    As a context manager::

        with ServiceRpcProxy('targetservice', config) as proxy:
            proxy.method()

    The equivalent call, manually starting and stopping::

        targetservice_proxy = ServiceRpcProxy('targetservice', config)
        proxy = targetservice_proxy.start()
        proxy.method()
        targetservice_proxy.stop()

    If you call ``start()`` you must eventually call ``stop()`` to close the
    connection to the broker.

    You may also supply ``context_data``, a dictionary of data to be
    serialised into the AMQP message headers, and specify custom worker
    context class to serialise them.
    """
    def __init__(self, service_name, *args, **kwargs):
        super(ServiceRpcProxy, self).__init__(*args, **kwargs)
        self._proxy = ServiceProxy(
            service_name, self._publish, self.reply_listener
        )


class ClusterProxy(object):
    """
    A single-threaded RPC proxy to a cluster of services. Individual services
    are accessed via attributes, which return service proxies. Method calls on
    the proxies are converted into RPC calls to the service, with responses
    returned directly.

    Enables services not hosted by nameko to make RPC requests to a nameko
    cluster. It is commonly used as a context manager but may also be manually
    started and stopped.

    This is similar to the service proxy, but may be uses a single reply queue
    for calls to all services, where a collection of service proxies would have
    one reply queue per proxy.

    *Usage*

    As a context manager::

        with ClusterRpcProxy(config) as proxy:
            proxy.service.method()
            proxy.other_service.method()

    The equivalent call, manually starting and stopping::

        proxy = ClusterRpcProxy(config)
        proxy = proxy.start()
        proxy.targetservice.method()
        proxy.other_service.method()
        proxy.stop()

    If you call ``start()`` you must eventually call ``stop()`` to close the
    connection to the broker.

    You may also supply ``context_data``, a dictionary of data to be
    serialised into the AMQP message headers.

    When the name of the service is not legal in Python, you can also
    use a dict-like syntax::

        with ClusterRpcProxy(config) as proxy:
            proxy['service-name'].method()
            proxy['other-service'].method()

    """

    def __init__(self, publish, reply_listener):
        self._publish = publish
        self._reply_listener = reply_listener

    def __getattr__(self, name):
        return ServiceProxy(
            name, self._publish, self._reply_listener
        )

    def __getitem__(self, name):
        """Enable dict-like access on the proxy. """
        return getattr(self, name)


class ClusterRpcProxy(StandaloneProxyBase):
    def __init__(self, *args, **kwargs):
        super(ClusterRpcProxy, self).__init__(*args, **kwargs)
        self._proxy = ClusterProxy(self._publish, self.reply_listener)
