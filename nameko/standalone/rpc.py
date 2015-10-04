from __future__ import absolute_import

import logging
import socket

from amqp.exceptions import ConnectionError
from kombu import Connection
from kombu.common import maybe_declare
from kombu.messaging import Consumer

from nameko.amqp import verify_amqp_uri
from nameko.constants import AMQP_URI_CONFIG_KEY
from nameko.containers import WorkerContext
from nameko.extensions import Entrypoint
from nameko.exceptions import RpcConnectionError, RpcTimeout
from nameko.rpc import ServiceProxy, ReplyListener
from nameko.constants import (
    SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER)


_logger = logging.getLogger(__name__)


class ConsumeEvent(object):
    """ Event for the RPC consumer with the same interface as eventlet.Event.
    """
    exception = None

    def __init__(self, queue_consumer, correlation_id):
        self.correlation_id = correlation_id
        self.queue_consumer = queue_consumer

    def send(self, body):
        self.body = body

    def send_exception(self, exc):
        self.exception = exc

    def wait(self):
        """ Makes a blocking call to its queue_consumer until the message
        with the given correlation_id has been processed.

        By the time the blocking call exits, self.send() will have been called
        with the body of the received message
        (see :meth:`~nameko.rpc.ReplyListener.handle_message`).

        Exceptions are raised directly.
        """
        # disconnected before starting to wait
        if self.exception:
            raise self.exception

        if self.queue_consumer.consumer.connection is None:
            raise RuntimeError(
                "This consumer has been stopped, and can no longer be used"
            )
        self.queue_consumer.get_message(self.correlation_id)

        # disconnected while waiting
        if self.exception:
            raise self.exception
        return self.body


class PollingQueueConsumer(object):
    """ Implements a minimum interface of the
    :class:`~messaging.QueueConsumer`. Instead of processing messages in a
    separate thread it provides a polling method to block until a message with
    the same correlation ID of the RPC-proxy call arrives.
    """
    consumer = None

    def __init__(self, timeout=None):
        self.timeout = timeout
        self.replies = {}

    def _setup_consumer(self):
        if self.consumer is not None:
            try:
                self.consumer.cancel()
            except socket.error:  # pragma: no cover
                # On some systems (e.g. os x) we need to explicitly cancel the
                # consumer here. However, e.g. on ubuntu 14.04, the
                # disconnection has already closed the socket. We try to
                # cancel, and ignore any socket errors.
                pass

        channel = self.connection.channel()
        # queue.bind returns a bound copy
        self.queue = self.queue.bind(channel)
        maybe_declare(self.queue, channel)
        consumer = Consumer(
            channel, queues=[self.queue], accept=self.accept, no_ack=False)
        consumer.callbacks = [self.on_message]
        consumer.consume()
        self.consumer = consumer

    def register_provider(self, provider):
        self.provider = provider

        self.serializer = provider.container.config.get(
            SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER)
        self.accept = [self.serializer]

        amqp_uri = provider.container.config[AMQP_URI_CONFIG_KEY]
        verify_amqp_uri(amqp_uri)
        self.connection = Connection(amqp_uri)

        self.queue = provider.queue
        self._setup_consumer()

    def unregister_provider(self, provider):
        self.connection.close()

    def ack_message(self, msg):
        msg.ack()

    def on_message(self, body, message):
        msg_correlation_id = message.properties.get('correlation_id')
        if msg_correlation_id not in self.provider._reply_events:
            _logger.debug(
                "Unknown correlation id: %s", msg_correlation_id)

        self.replies[msg_correlation_id] = (body, message)

    def get_message(self, correlation_id):

        try:
            while correlation_id not in self.replies:
                self.consumer.channel.connection.client.drain_events(
                    timeout=self.timeout
                )

            body, message = self.replies.pop(correlation_id)
            self.provider.handle_message(body, message)

        except socket.timeout:
            timeout_error = RpcTimeout(self.timeout)
            event = self.provider._reply_events.pop(correlation_id)
            event.send_exception(timeout_error)

            # timeout is implemented using socket timeout, so when it
            # fires the connection is closed, causing the reply queue
            # to be deleted
            self._setup_consumer()

        except ConnectionError as exc:
            for event in self.provider._reply_events.values():
                rpc_connection_error = RpcConnectionError(
                    'Disconnected while waiting for reply: %s', exc)
                event.send_exception(rpc_connection_error)
            self.provider._reply_events.clear()
            # In case this was a temporary error, attempt to reconnect. If
            # we fail, the connection error will bubble.
            self._setup_consumer()

        except KeyboardInterrupt as exc:
            event = self.provider._reply_events.pop(correlation_id)
            event.send_exception(exc)
            # exception may have killed the connection
            self._setup_consumer()


class SingleThreadedReplyListener(ReplyListener):
    """ A ReplyListener which uses a custom queue consumer and ConsumeEvent.
    """
    queue_consumer = None

    def __init__(self, timeout=None):
        self.queue_consumer = PollingQueueConsumer(timeout=timeout)
        super(SingleThreadedReplyListener, self).__init__()

    def get_reply_event(self, correlation_id):
        reply_event = ConsumeEvent(self.queue_consumer, correlation_id)
        self._reply_events[correlation_id] = reply_event
        return reply_event


class StandaloneProxyBase(object):
    class ServiceContainer(object):
        """ Implements a minimum interface of the
        :class:`~containers.ServiceContainer` to be used by the subclasses
        and rpc imports in this module.
        """
        service_name = "standalone_rpc_proxy"

        def __init__(self, config):
            self.config = config
            self.shared_extensions = {}

    class Dummy(Entrypoint):
        method_name = "call"

    _proxy = None

    def __init__(
        self, config, context_data=None, timeout=None,
        worker_ctx_cls=WorkerContext
    ):
        container = self.ServiceContainer(config)

        reply_listener = SingleThreadedReplyListener(timeout=timeout).bind(
            container)

        self._worker_ctx = worker_ctx_cls(
            container, service=None, entrypoint=self.Dummy,
            data=context_data)
        self._reply_listener = reply_listener

    def __enter__(self):
        return self.start()

    def __exit__(self, tpe, value, traceback):
        self.stop()

    def start(self):
        self._reply_listener.setup()
        return self._proxy

    def stop(self):
        self._reply_listener.stop()


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
            self._worker_ctx, service_name, self._reply_listener)


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
    serialised into the AMQP message headers, and specify custom worker
    context class to serialise them.
    """
    def __init__(self, worker_ctx, reply_listener):
        self._worker_ctx = worker_ctx
        self._reply_listener = reply_listener

        self._proxies = {}

    def __getattr__(self, name):
        if name not in self._proxies:
            self._proxies[name] = ServiceProxy(
                self._worker_ctx, name, self._reply_listener)
        return self._proxies[name]


class ClusterRpcProxy(StandaloneProxyBase):
    def __init__(self, *args, **kwargs):
        super(ClusterRpcProxy, self).__init__(*args, **kwargs)
        self._proxy = ClusterProxy(self._worker_ctx, self._reply_listener)
