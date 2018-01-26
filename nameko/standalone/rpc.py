from __future__ import absolute_import

import logging
import socket
import uuid

from kombu import Connection
from kombu.common import maybe_declare
from kombu.messaging import Queue
from kombu.mixins import ConsumerMixin

from nameko.amqp import verify_amqp_uri
from nameko.amqp.publish import Publisher
from nameko.constants import (
    AMQP_URI_CONFIG_KEY, DEFAULT_HEARTBEAT, DEFAULT_SERIALIZER,
    HEARTBEAT_CONFIG_KEY, SERIALIZER_CONFIG_KEY
)
from nameko.containers import new_call_id
from nameko.exceptions import RpcTimeout
from nameko.messaging import encode_to_headers
from nameko.rpc import (
    RPC_REPLY_QUEUE_TEMPLATE, RPC_REPLY_QUEUE_TTL, ServiceProxy,
    get_rpc_exchange
)


_logger = logging.getLogger(__name__)


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
            try:
                next(self.consume(timeout=self.timeout))
            except socket.timeout:
                raise RpcTimeout()
        res = self.pending.pop(correlation_id)
        return res

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
        data = context_data

        serializer = config.get(SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER)
        # options?

        publisher = Publisher(
            self.amqp_uri,
            serializer=serializer
        )

        def publish(*args, **kwargs):
            # can we get a nicer api than passing in a publish function?\
            # e.g. an "invoke" func?

            context_data = data or {}
            context_data['call_id_stack'] = [
                # TODO: replace "call" with uuid of proxy
                'standalone_rpc_proxy.call.{}'.format(new_call_id())
            ]

            extra_headers = encode_to_headers(context_data)

            publisher.publish(
                *args, exchange=exchange, extra_headers=extra_headers, **kwargs
            )

        self._publish = publish

    @property
    def amqp_uri(self):
        return self.config[AMQP_URI_CONFIG_KEY]

    def __enter__(self):
        return self.start()

    def __exit__(self, tpe, value, traceback):
        self.stop()

    def start(self):
        self.reply_listener.start()
        return self._proxy  # set in subclass __init__

    def stop(self):
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
