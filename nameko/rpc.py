from __future__ import absolute_import, unicode_literals

import sys
import uuid
from functools import partial
from logging import getLogger

from eventlet.event import Event
from eventlet.queue import Empty
from kombu import Connection, Exchange, Queue
from kombu.pools import producers
import kombu.serialization

from nameko.constants import (
    AMQP_URI_CONFIG_KEY, DEFAULT_RETRY_POLICY, RPC_EXCHANGE_CONFIG_KEY,
    SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER)
from nameko.exceptions import (
    ContainerBeingKilled, deserialize, MalformedRequest, MethodNotFound,
    RpcConnectionError, serialize, UnknownService, UnserializableValueError)
from nameko.extensions import (
    DependencyProvider, Entrypoint, ProviderCollector, SharedExtension)
from nameko.messaging import HeaderDecoder, HeaderEncoder, QueueConsumer

_log = getLogger(__name__)


RPC_QUEUE_TEMPLATE = 'rpc-{}'
RPC_REPLY_QUEUE_TEMPLATE = 'rpc.reply-{}-{}'


def get_rpc_exchange(config):
    exchange_name = config.get(RPC_EXCHANGE_CONFIG_KEY, 'nameko-rpc')
    exchange = Exchange(exchange_name, durable=True, type="topic")
    return exchange


class RpcConsumer(SharedExtension, ProviderCollector):

    queue_consumer = QueueConsumer()

    def __init__(self):
        self._unregistering_providers = set()
        self._unregistered_from_queue_consumer = Event()
        self.queue = None
        super(RpcConsumer, self).__init__()

    def setup(self):
        if self.queue is None:

            service_name = self.container.service_name
            queue_name = RPC_QUEUE_TEMPLATE.format(service_name)
            routing_key = '{}.*'.format(service_name)

            exchange = get_rpc_exchange(self.container.config)

            self.queue = Queue(
                queue_name,
                exchange=exchange,
                routing_key=routing_key,
                durable=True)

            self.queue_consumer.register_provider(self)
            self._registered = True

    def stop(self):
        """ Stop the RpcConsumer.

        The RpcConsumer ordinary unregisters from the QueueConsumer when the
        last Rpc subclass unregisters from it. If no providers were registered,
        we should unregister from the QueueConsumer as soon as we're asked
        to stop.
        """
        if not self._providers_registered:
            self.queue_consumer.unregister_provider(self)
            self._unregistered_from_queue_consumer.send(True)

    def unregister_provider(self, provider):
        """ Unregister a provider.

        Blocks until this RpcConsumer is unregistered from its QueueConsumer,
        which only happens when all providers have asked to unregister.
        """
        self._unregistering_providers.add(provider)
        remaining_providers = self._providers - self._unregistering_providers
        if not remaining_providers:
            _log.debug('unregistering from queueconsumer %s', self)
            self.queue_consumer.unregister_provider(self)
            _log.debug('unregistered from queueconsumer %s', self)
            self._unregistered_from_queue_consumer.send(True)

        _log.debug('waiting for unregister from queue consumer %s', self)
        self._unregistered_from_queue_consumer.wait()
        super(RpcConsumer, self).unregister_provider(provider)

    def get_provider_for_method(self, routing_key):
        service_name = self.container.service_name

        for provider in self._providers:
            key = '{}.{}'.format(service_name, provider.method_name)
            if key == routing_key:
                return provider
        else:
            method_name = routing_key.split(".")[-1]
            raise MethodNotFound(method_name)

    def handle_message(self, body, message):
        routing_key = message.delivery_info['routing_key']
        try:
            provider = self.get_provider_for_method(routing_key)
            provider.handle_message(body, message)
        except Exception:
            exc_info = sys.exc_info()
            self.handle_result(message, None, exc_info)

    def handle_result(self, message, result, exc_info):
        responder = Responder(self.container.config, message)
        result, exc_info = responder.send_response(result, exc_info)

        self.queue_consumer.ack_message(message)
        return result, exc_info

    def requeue_message(self, message):
        self.queue_consumer.requeue_message(message)


class Rpc(Entrypoint, HeaderDecoder):

    rpc_consumer = RpcConsumer()

    def __init__(self, expected_exceptions=(), sensitive_variables=()):
        """ Mark a method to be exposed over rpc

        :Parameters:
            expected_exceptions : exception class or tuple of exception classes
                Specify exceptions that may be caused by the caller (e.g. by
                providing bad arguments). Saved on the entrypoint instance as
                ``entrypoint.expected_exceptions`` for later inspection by
                other extensions, for example a monitoring system.
            sensitive_variables : string or tuple of strings
                Mark an argument or part of an argument as sensitive. Saved on
                the entrypoint instance as ``entrypoint.sensitive_variables``
                for later inspection by other extensions, for example a
                logging system.

                :seealso: :func:`nameko.utils.get_redacted_args`

        """
        self.expected_exceptions = expected_exceptions
        self.sensitive_variables = sensitive_variables

    def setup(self):
        self.rpc_consumer.register_provider(self)

    def stop(self):
        self.rpc_consumer.unregister_provider(self)

    def handle_message(self, body, message):
        try:
            args = body['args']
            kwargs = body['kwargs']
        except KeyError:
            raise MalformedRequest('Message missing `args` or `kwargs`')

        self.check_signature(args, kwargs)

        worker_ctx_cls = self.container.worker_ctx_cls
        context_data = self.unpack_message_headers(worker_ctx_cls, message)

        handle_result = partial(self.handle_result, message)
        try:
            self.container.spawn_worker(self, args, kwargs,
                                        context_data=context_data,
                                        handle_result=handle_result)
        except ContainerBeingKilled:
            self.rpc_consumer.requeue_message(message)

    def handle_result(self, message, worker_ctx, result, exc_info):
        result, exc_info = self.rpc_consumer.handle_result(
            message, result, exc_info)
        return result, exc_info


rpc = Rpc.decorator


class Responder(object):

    def __init__(self, config, message):
        self.config = config
        self.message = message

    def send_response(self, result, exc_info, **kwargs):

        error = None
        if exc_info is not None:
            error = serialize(exc_info[1])

        # disaster avoidance serialization check: `result` must be
        # serializable, otherwise the container will commit suicide assuming
        # unrecoverable errors (and the message will be requeued for another
        # victim)

        serializer = self.config.get(
            SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER)

        try:
            kombu.serialization.dumps(result, serializer)
        except Exception:
            exc_info = sys.exc_info()
            # `error` below is guaranteed to serialize to json
            error = serialize(UnserializableValueError(result))
            result = None

        conn = Connection(self.config[AMQP_URI_CONFIG_KEY])

        exchange = get_rpc_exchange(self.config)

        retry = kwargs.pop('retry', True)
        retry_policy = kwargs.pop('retry_policy', DEFAULT_RETRY_POLICY)

        with producers[conn].acquire(block=True) as producer:

            routing_key = self.message.properties['reply_to']
            correlation_id = self.message.properties.get('correlation_id')

            msg = {'result': result, 'error': error}

            _log.debug('publish response %s:%s', routing_key, correlation_id)
            producer.publish(
                msg, retry=retry, retry_policy=retry_policy,
                exchange=exchange, routing_key=routing_key,
                serializer=serializer,
                correlation_id=correlation_id, **kwargs)

        return result, exc_info


class ReplyListener(SharedExtension):

    queue_consumer = QueueConsumer()

    def __init__(self, **kwargs):
        self._reply_events = {}
        super(ReplyListener, self).__init__(**kwargs)

    def setup(self):

        service_uuid = uuid.uuid4()  # TODO: give srv_ctx a uuid?
        service_name = self.container.service_name

        queue_name = RPC_REPLY_QUEUE_TEMPLATE.format(
            service_name, service_uuid)

        self.routing_key = str(service_uuid)

        exchange = get_rpc_exchange(self.container.config)

        self.queue = Queue(
            queue_name,
            exchange=exchange,
            routing_key=self.routing_key,
            auto_delete=True,
            exclusive=True,
        )

        self.queue_consumer.register_provider(self)

    def stop(self):
        self.queue_consumer.unregister_provider(self)
        super(ReplyListener, self).stop()

    def get_reply_event(self, correlation_id):
        reply_event = Event()
        self._reply_events[correlation_id] = reply_event
        return reply_event

    def on_consume_ready(self):
        # This is called on re-connection, and is the best hook for detecting
        # disconnections. If we have any pending reply events, we were
        # disconnected, and may have lost replies (since reply queues auto
        # delete).
        for event in self._reply_events.values():
            event.send_exception(
                RpcConnectionError('Disconnected while waiting for reply')
            )
        self._reply_events.clear()

    def handle_message(self, body, message):
        self.queue_consumer.ack_message(message)

        correlation_id = message.properties.get('correlation_id')
        client_event = self._reply_events.pop(correlation_id, None)
        if client_event is not None:
            client_event.send(body)
        else:
            _log.debug("Unknown correlation id: %s", correlation_id)


class RpcProxy(DependencyProvider):

    rpc_reply_listener = ReplyListener()

    def __init__(self, target_service):
        self.target_service = target_service

    def get_dependency(self, worker_ctx):
        return ServiceProxy(worker_ctx, self.target_service,
                            self.rpc_reply_listener)


class ServiceProxy(object):
    def __init__(self, worker_ctx, service_name, reply_listener):
        self.worker_ctx = worker_ctx
        self.service_name = service_name
        self.reply_listener = reply_listener

    def __getattr__(self, name):
        return MethodProxy(
            self.worker_ctx, self.service_name, name, self.reply_listener)


class RpcReply(object):
    resp_body = None

    def __init__(self, reply_event):
        self.reply_event = reply_event

    def result(self):
        _log.debug('Waiting for RPC reply event %s', self)

        if self.resp_body is None:
            self.resp_body = self.reply_event.wait()
            _log.debug('RPC reply event complete %s %s', self, self.resp_body)

        error = self.resp_body.get('error')
        if error:
            raise deserialize(error)
        return self.resp_body['result']


class MethodProxy(HeaderEncoder):

    def __init__(self, worker_ctx, service_name, method_name, reply_listener):
        self.worker_ctx = worker_ctx
        self.service_name = service_name
        self.method_name = method_name
        self.reply_listener = reply_listener

    def __call__(self, *args, **kwargs):
        reply = self._call(*args, **kwargs)
        return reply.result()

    def async(self, *args, **kwargs):
        reply = self._call(*args, **kwargs)
        return reply

    def _call(self, *args, **kwargs):
        _log.debug('invoking %s', self)

        worker_ctx = self.worker_ctx
        container = worker_ctx.container

        msg = {'args': args, 'kwargs': kwargs}

        conn = Connection(
            container.config[AMQP_URI_CONFIG_KEY],
            transport_options={'confirm_publish': True},
        )

        serializer = container.config.get(
            SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER)

        # We use the `mandatory` flag in `producer.publish` below to catch rpc
        # calls to non-existent services, which would otherwise wait forever
        # for a reply that will never arrive.
        #
        # However, the basic.return ("no one is listening for topic") is sent
        # asynchronously and conditionally, so we can't wait() on the channel
        # for it (will wait forever on successful delivery).
        #
        # Instead, we use (the rabbitmq extension) confirm_publish
        # (https://www.rabbitmq.com/confirms.html), which _always_ sends a
        # reply down the channel. Moreover, in the case where no queues are
        # bound to the exchange (service unknown), the basic.return is sent
        # first, so by the time kombu returns (after waiting for the confim)
        # we can reliably check for returned messages.

        routing_key = '{}.{}'.format(self.service_name, self.method_name)

        exchange = get_rpc_exchange(container.config)

        with producers[conn].acquire(block=True) as producer:

            headers = self.get_message_headers(worker_ctx)
            correlation_id = str(uuid.uuid4())

            reply_listener = self.reply_listener
            reply_to_routing_key = reply_listener.routing_key
            reply_event = reply_listener.get_reply_event(correlation_id)

            producer.publish(
                msg,
                exchange=exchange,
                routing_key=routing_key,
                mandatory=True,
                serializer=serializer,
                reply_to=reply_to_routing_key,
                headers=headers,
                correlation_id=correlation_id,
                retry=True,
                retry_policy=DEFAULT_RETRY_POLICY
            )

            # This used to do .empty() to check if the queue is empty
            # but we actually need to clear out the queue here as
            # otherwise future code that reuses the same producer will
            # incorrectly see a failure which actually was an earlier
            # one.
            try:
                producer.channel.returned_messages.get_nowait()
            except Empty:
                pass
            else:
                raise UnknownService(self.service_name)

        return RpcReply(reply_event)

    def __repr__(self):
        service_name = self.service_name
        method_name = self.method_name
        return '<proxy method: {}.{}>'.format(service_name, method_name)
