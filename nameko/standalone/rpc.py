from __future__ import absolute_import

import logging
import socket
import time

from amqp.exceptions import ConnectionError
from kombu import Connection
from kombu.common import maybe_declare
from kombu.messaging import Consumer
from nameko import serialization
from nameko.constants import AMQP_SSL_CONFIG_KEY, AMQP_URI_CONFIG_KEY, HEARTBEAT_CONFIG_KEY
from nameko.containers import WorkerContext
from nameko.exceptions import RpcTimeout, ConfigurationError
from nameko.extensions import Entrypoint
from nameko.rpc import ReplyListener, ServiceProxy

_logger = logging.getLogger(__name__)

EXPECTED_IOERR_MSG_SET = {"Server unexpectedly closed connection"}


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

        if self.queue_consumer.stopped:
            raise RuntimeError(
                "This consumer has been stopped, and can no longer be used"
            )
        if self.queue_consumer.connection is None or \
                self.queue_consumer.connection.connected is False:
            # we can't just reconnect here. the consumer (and its exclusive,
            # auto-delete reply queue) must be re-established _before_ sending
            # any request, otherwise the reply queue may not exist when the
            # response is published.
            raise RuntimeError(
                "This consumer has been disconnected, and can no longer "
                "be used"
            )

        try:
            self.queue_consumer.get_message(self.correlation_id)
        except socket.error as exc:
            self.exception = exc

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
    connection = None

    def __init__(self, timeout=None):
        self.stopped = True
        self.timeout = timeout
        self.replies = {}

    def _setup_consumer(self):
        if self.consumer is not None:
            try:
                self.consumer.cancel()
            except (socket.error, IOError):  # pragma: no cover
                # On some systems (e.g. os x) we need to explicitly cancel the
                # consumer here. However, e.g. on ubuntu 14.04, the
                # disconnection has already closed the socket. We try to
                # cancel, and ignore any socket errors.
                # If the socket has been closed, an IOError is raised, ignore
                # it and assume the consumer is already cancelled.
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

    def _setup_connection(self):
        if self.connection is not None:
            try:
                self.connection.close()
            except (socket.error, IOError):  # pragma: no cover
                # If the socket has been closed, an IOError is raised, ignore
                # it and assume the connection is already closed.
                pass

        amqp_uri = self.provider.container.config[AMQP_URI_CONFIG_KEY]
        ssl = self.provider.container.config.get(AMQP_SSL_CONFIG_KEY)
        self.heartbeat = self.provider.container.config.get(
            HEARTBEAT_CONFIG_KEY, None  # default to not enable heartbeat
        )
        if self.heartbeat is not None and self.heartbeat < 0:
            raise ConfigurationError("value for '%s' can not be negative" % HEARTBEAT_CONFIG_KEY)
        self.connection = Connection(amqp_uri, ssl=ssl, heartbeat=self.heartbeat)

    def register_provider(self, provider):
        self.provider = provider

        self.serializer, self.accept = serialization.setup(
            provider.container.config)

        self._setup_connection()

        self.queue = provider.queue
        self._setup_consumer()
        self.stopped = False

    def unregister_provider(self, provider):
        self.connection.close()
        self.stopped = True

    def ack_message(self, message):
        # only attempt to ack if the message connection is alive;
        # otherwise the message will already have been reclaimed by the broker
        if message.channel.connection:
            try:
                message.ack()
            except ConnectionError:  # pragma: no cover
                pass  # ignore connection closing inside conditional

    def on_message(self, body, message):
        msg_correlation_id = message.properties.get('correlation_id')
        unknown = False
        if msg_correlation_id not in self.provider._reply_events:
            _logger.debug(
                "Unknown correlation id: %s", msg_correlation_id)
            unknown = True
        self.replies[msg_correlation_id] = (body, message)
        # ACK message here to notify broker that message has been received
        # only if the msg is known by the consumer
        if not unknown:
            self.ack_message(message)


    def get_message(self, correlation_id):
        start_time = time.time()
        stop_waiting = False
        # retrieve agreed heartbeat interval of both party by query the server
        HEARTBEAT_INTERVAL = self.consumer.connection.get_heartbeat_interval()
        RATE = lambda: min(2 + abs(time.time() - start_time) * .75 / HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL / 3)
        true_timeout = lambda: abs(start_time + self.timeout - time.time()) if self.timeout is not None else None
        remaining_timeout = lambda: (
            min(abs(start_time + self.timeout - time.time()), HEARTBEAT_INTERVAL / RATE())
            if self.timeout is not None else HEARTBEAT_INTERVAL / RATE()) if self.heartbeat else true_timeout()
        is_timed_out = lambda: abs(time.time() - start_time) > self.timeout if self.timeout is not None else False
        timed_out_err_msg = "Timeout after: {}".format(self.timeout)

        while correlation_id not in self.replies:
            recover_connection = False
            try:
                if self.heartbeat:
                    try:
                        self.consumer.connection.heartbeat_check()
                    except (ConnectionError, socket.error, IOError) as exc:
                        _logger.info("Heart beat failed. System will auto recover broken connection, %s: %s",
                                     type(exc).__name__, exc.args[0])
                        raise
                    else:
                        _logger.debug("Heart beat OK")
                self.consumer.connection.drain_events(timeout=remaining_timeout())
            except socket.timeout:
                # if socket timeout happen here, send a hearbeat and keep looping
                # until self.timeout is reached or correlation_id is found
                pass
            except (ConnectionError, socket.error, IOError) as exc:
                # in case this was a temporary error, attempt to reconnect
                # and try again. if we fail to reconnect, the error will bubble
                # wait till connection stable before retry
                if isinstance(exc, IOError) and not isinstance(exc, socket.error):
                    # check only certain IOError will attemt to reconnect otherwhile reraise
                    if exc.args[0] not in EXPECTED_IOERR_MSG_SET:  # check error.message
                        raise
                if not is_timed_out():
                    try:  # try to recover connection if there is still time
                        recover_connection = True
                        _logger.debug(
                            "Stabilizing connection to message broker due to error, {}: {}".format(type(exc).__name__,
                                                                                                   exc.args[0]))
                        self._setup_connection()
                        self.connection.ensure_connection(max_retries=2, timeout=true_timeout())
                        if self.connection.connected is True:
                            self._setup_consumer()
                            recover_connection = False
                            # continue the loop to start send a heartbeat and wait result with this new connection
                        else:
                            err_msg = "Unable to stabilizing connnection after error, {}: {}".format(type(exc).__name__,
                                                                                                     exc.args[0])
                            _logger.debug(err_msg)
                            event = self.provider._reply_events.pop(correlation_id)
                            event.send_exception(ConnectionError(err_msg))
                            stop_waiting = True
                    except socket.timeout:
                        timed_out_err_msg = "Timeout after stabilizing connnection: {}".format(self.timeout)
                        # continue the loop to try to recover connection until
                        # either self.timeout is reached or correlation_id is found
                    except (ConnectionError, socket.error) as exc2:
                        err_msg = "Error during stabilizing connnection, {}: {}".format(type(exc2).__name__,
                                                                                        exc2.args[0])
                        _logger.debug(err_msg)
                        event = self.provider._reply_events.pop(correlation_id)
                        event.send_exception(ConnectionError(err_msg))
                        recover_connection = True
                        stop_waiting = True
            except KeyboardInterrupt as exc:
                event = self.provider._reply_events.pop(correlation_id)
                event.send_exception(exc)
                recover_connection = True
                stop_waiting = True
            finally:
                if correlation_id in self.replies:
                    body, message = self.replies.pop(correlation_id)
                    self.provider.handle_message(body, message)
                    stop_waiting = True
                else:
                    if is_timed_out() is True:
                        _logger.debug(timed_out_err_msg)
                        timeout_error = RpcTimeout(timed_out_err_msg)
                        event = self.provider._reply_events.pop(correlation_id)
                        event.send_exception(timeout_error)
                        stop_waiting = True
                if recover_connection:  # alway try to recover the connection before exit if this flag is True
                    try:
                        if self.connection.connected is False:
                            self._setup_connection()
                        self._setup_consumer()
                    except socket.error as exc:
                        _logger.debug("Socket error during setup consumer, %s: %s", type(exc).__name__, exc.args[0])
                if stop_waiting:  # stop waiting for result, break the loop
                    break
        else:  # other thread may have receive the message coresponding to this correlation_id before enter wait loop
            body, message = self.replies.pop(correlation_id)
            self.provider.handle_message(body, message)


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

    def handle_message(self, body, message):
        # attempt to ACK message if it hasn't been acked
        if not message.acknowledged:
            self.queue_consumer.ack_message(message)

        correlation_id = message.properties.get('correlation_id')
        # correlation_id must exist in _reply_events by the time this method is called
        client_event = self._reply_events[correlation_id]
        client_event.send(body)


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
        reply_listener_cls=SingleThreadedReplyListener
    ):
        container = self.ServiceContainer(config)

        self._worker_ctx = WorkerContext(
            container, service=None, entrypoint=self.Dummy,
            data=context_data)
        self._reply_listener = reply_listener_cls(
            timeout=timeout).bind(container)

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

    When the name of the service is not legal in Python, you can also
    use a dict-like syntax::

        with ClusterRpcProxy(config) as proxy:
            proxy['service-name'].method()
            proxy['other-service'].method()

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

    def __getitem__(self, name):
        """Enable dict-like access on the proxy. """
        return getattr(self, name)


class ClusterRpcProxy(StandaloneProxyBase):
    def __init__(self, *args, **kwargs):
        super(ClusterRpcProxy, self).__init__(*args, **kwargs)
        self._proxy = ClusterProxy(self._worker_ctx, self._reply_listener)
