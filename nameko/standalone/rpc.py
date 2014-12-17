from __future__ import absolute_import

import logging

from amqp.exceptions import ConnectionError
from kombu import Connection
from kombu.common import maybe_declare

from nameko.containers import WorkerContext
from nameko.exceptions import RpcConnectionError, RpcTimeout
from nameko.kombu_helpers import queue_iterator
from nameko.rpc import ServiceProxy, ReplyListener


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
        (see :class:nameko.rpc.ReplyListener.handle_message).

        Exceptions are raised directly.
        """
        # disconnected before starting to wait
        if self.exception:
            raise self.exception

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
    def __init__(self, timeout=None):
        self.timeout = timeout

    def _setup_queue(self):
        self.channel = self.connection.channel()
        # queue.bind returns a bound copy
        self.queue = self.queue.bind(self.channel)
        maybe_declare(self.queue, self.channel)

    def register_provider(self, provider):
        self.provider = provider
        self.connection = Connection(provider.container.config['AMQP_URI'])
        self.queue = provider.queue
        self._setup_queue()
        message_iterator = self._poll_messages()
        message_iterator.send(None)  # start generator
        self.get_message = message_iterator.send

    def unregister_provider(self, provider):
        self.connection.close()

    def ack_message(self, msg):
        msg.ack()

    def _poll_messages(self):
        replies = {}

        correlation_id = yield

        while True:
            try:
                for body, msg in queue_iterator(
                    self.queue, timeout=self.timeout
                ):
                    msg_correlation_id = msg.properties.get('correlation_id')

                    if msg_correlation_id not in self.provider._reply_events:
                        _logger.debug(
                            "Unknown correlation id: %s", msg_correlation_id)
                        continue

                    replies[msg_correlation_id] = (body, msg)

                    # Here, and every time we re-enter this coroutine (at the
                    # `yield` statement below) we check if we already have the
                    # data for the new correlation_id before polling for new
                    # messages.
                    while correlation_id in replies:
                        body, msg = replies.pop(correlation_id)
                        self.provider.handle_message(body, msg)
                        correlation_id = yield

            except RpcTimeout as exc:
                event = self.provider._reply_events.pop(correlation_id)
                event.send_exception(exc)

                # timeout is implemented using socket timeout, so when it
                # fires the connection is closed, causing the reply queue
                # to be deleted
                self._setup_queue()
                correlation_id = yield

            except ConnectionError as exc:
                for event in self.provider._reply_events.values():
                    rpc_connection_error = RpcConnectionError(
                        'Disconnected while waiting for reply: %s', exc)
                    event.send_exception(rpc_connection_error)
                self.provider._reply_events.clear()
                # In case this was a temporary error, attempt to reconnect. If
                # we fail, the connection error will bubble.
                self._setup_queue()
                correlation_id = yield


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


class RpcProxy(object):
    """
    A single-threaded RPC proxy to a named service. Method calls on the
    proxy are converted into RPC calls to the service, with responses
    returned directly.

    Enables services not hosted by nameko to make RPC requests to a nameko
    cluster. It is commonly used as a context manager but may also be manually
    started and stopped.

    *Usage*

    As a context manager::

        with RpcProxy('targetservice', config) as proxy:
            proxy.method()

    The equivalent call, manually starting and stopping::

        targetservice_proxy = RpcProxy('targetservice', config)
        proxy = targetservice_proxy.start()
        proxy.method()
        targetservice_proxy.stop()

    If you call ``start()`` you must eventually call ``stop()`` to close the
    connection to the broker.

    You may also supply ``context_data``, a dictionary of data to be
    serialised into the AMQP message headers, and specify custom worker
    context class to serialise them.
    """
    class ServiceContainer(object):
        """ Implements a minimum interface of the
        :class:`~containers.ServiceContainer` to be used by the subclasses
        and rpc imports in this module.
        """
        service_name = "standalone_rpc_proxy"

        def __init__(self, config):
            self.config = config

    class DummyProvider(object):
        name = "call"

    def __init__(
        self, container_service_name, config, context_data=None, timeout=None,
        worker_ctx_cls=WorkerContext
    ):

        container = RpcProxy.ServiceContainer(config)

        reply_listener = SingleThreadedReplyListener(timeout=timeout)
        reply_listener.container = container

        worker_ctx = worker_ctx_cls(
            container, service=None, provider=self.DummyProvider,
            data=context_data)
        service_proxy = ServiceProxy(worker_ctx, container_service_name,
                                     reply_listener)

        self._reply_listener = reply_listener
        self._service_proxy = service_proxy

    def __enter__(self):
        self.start()
        return self._service_proxy

    def __exit__(self, tpe, value, traceback):
        self.stop()

    def start(self):
        self._reply_listener.prepare()
        return self._service_proxy

    def stop(self):
        self._reply_listener.stop()
