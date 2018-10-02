import warnings
from contextlib import contextmanager

from kombu import Connection
from kombu.exceptions import ChannelError
from kombu.pools import connections, producers

from nameko.constants import (
    DEFAULT_RETRY_POLICY, DEFAULT_TRANSPORT_OPTIONS, PERSISTENT
)


class UndeliverableMessage(Exception):
    """ Raised when publisher confirms are enabled and a message could not
    be routed or persisted """
    pass


@contextmanager
def get_connection(amqp_uri, ssl=None, transport_options=None):
    if not transport_options:
        transport_options = DEFAULT_TRANSPORT_OPTIONS.copy()
    conn = Connection(amqp_uri, transport_options=transport_options, ssl=ssl)

    with connections[conn].acquire(block=True) as connection:
        yield connection


@contextmanager
def get_producer(amqp_uri, confirms=True, ssl=None, transport_options=None):
    if transport_options is None:
        transport_options = DEFAULT_TRANSPORT_OPTIONS.copy()
    transport_options['confirm_publish'] = confirms
    conn = Connection(amqp_uri, transport_options=transport_options, ssl=ssl)

    with producers[conn].acquire(block=True) as producer:
        yield producer


class Publisher(object):
    """
    Utility helper for publishing messages to RabbitMQ.
    """

    use_confirms = True
    """
    Enable `confirms <http://www.rabbitmq.com/confirms.html>`_ for this
    publisher.

    The publisher will wait for an acknowledgement from the broker that
    the message was received and processed appropriately, and otherwise
    raise. Confirms have a performance penalty but guarantee that messages
    aren't lost, for example due to stale connections.
    """

    transport_options = DEFAULT_TRANSPORT_OPTIONS.copy()
    """
    A dict of additional connection arguments to pass to alternate kombu
    channel implementations. Consult the transport documentation for
    available options.
    """

    delivery_mode = PERSISTENT
    """
    Default delivery mode for messages published by this Publisher.
    """

    mandatory = False
    """
    Require `mandatory <https://www.rabbitmq.com/amqp-0-9-1-reference.html
    #basic.publish.mandatory>`_ delivery for published messages.
    """

    priority = 0
    """
    Priority value for published messages, to be used in conjunction with
    `consumer priorities <https://www.rabbitmq.com/priority.html>_`.
    """

    expiration = None
    """
    `Per-message TTL <https://www.rabbitmq.com/ttl.html>`_, in milliseconds.
    """

    serializer = "json"
    """ Name of the serializer to use when publishing messages.

    Must be registered as a
    `kombu serializer <http://bit.do/kombu_serialization>`_.
    """

    compression = None
    """ Name of the compression to use when publishing messages.

    Must be registered as a
    `kombu compression utility <http://bit.do/kombu-compression>`_.
    """

    retry = True
    """
    Enable automatic retries when publishing a message that fails due
    to a connection error.

    Retries according to :attr:`self.retry_policy`.
    """

    retry_policy = DEFAULT_RETRY_POLICY
    """
    Policy to apply when retrying message publishes, if requested.

    See :attr:`self.retry`.
    """

    declare = []
    """
    Kombu :class:`~kombu.messaging.Queue` or :class:`~kombu.messaging.Exchange`
    objects to (re)declare before publishing a message.
    """

    def __init__(
        self, amqp_uri, use_confirms=None, serializer=None, compression=None,
        delivery_mode=None, mandatory=None, priority=None, expiration=None,
        declare=None, retry=None, retry_policy=None, ssl=None, **publish_kwargs
    ):
        self.amqp_uri = amqp_uri
        self.ssl = ssl

        # publish confirms
        if use_confirms is not None:
            self.use_confirms = use_confirms

        # delivery options
        if delivery_mode is not None:
            self.delivery_mode = delivery_mode
        if mandatory is not None:
            self.mandatory = mandatory
        if priority is not None:
            self.priority = priority
        if expiration is not None:
            self.expiration = expiration

        # message options
        if serializer is not None:
            self.serializer = serializer
        if compression is not None:
            self.compression = compression

        # retry policy
        if retry is not None:
            self.retry = retry
        if retry_policy is not None:
            self.retry_policy = retry_policy

        # declarations
        if declare is not None:
            self.declare = declare

        # other publish arguments
        self.publish_kwargs = publish_kwargs

    def publish(self, payload, **kwargs):
        """ Publish a message.
        """
        publish_kwargs = self.publish_kwargs.copy()

        # merge headers from when the publisher was instantiated
        # with any provided now; "extra" headers always win
        headers = publish_kwargs.pop('headers', {}).copy()
        headers.update(kwargs.pop('headers', {}))
        headers.update(kwargs.pop('extra_headers', {}))

        use_confirms = kwargs.pop('use_confirms', self.use_confirms)
        transport_options = kwargs.pop('transport_options',
                                       self.transport_options
                                       )
        transport_options['confirm_publish'] = use_confirms

        delivery_mode = kwargs.pop('delivery_mode', self.delivery_mode)
        mandatory = kwargs.pop('mandatory', self.mandatory)
        priority = kwargs.pop('priority', self.priority)
        expiration = kwargs.pop('expiration', self.expiration)
        serializer = kwargs.pop('serializer', self.serializer)
        compression = kwargs.pop('compression', self.compression)
        retry = kwargs.pop('retry', self.retry)
        retry_policy = kwargs.pop('retry_policy', self.retry_policy)

        declare = self.declare[:]
        declare.extend(kwargs.pop('declare', ()))

        publish_kwargs.update(kwargs)  # remaining publish-time kwargs win

        with get_producer(self.amqp_uri,
                          use_confirms,
                          self.ssl,
                          transport_options,
                          ) as producer:
            try:
                producer.publish(
                    payload,
                    headers=headers,
                    delivery_mode=delivery_mode,
                    mandatory=mandatory,
                    priority=priority,
                    expiration=expiration,
                    compression=compression,
                    declare=declare,
                    retry=retry,
                    retry_policy=retry_policy,
                    serializer=serializer,
                    **publish_kwargs
                )
            except ChannelError as exc:
                if "NO_ROUTE" in str(exc):
                    raise UndeliverableMessage()
                raise

            if mandatory:
                if not use_confirms:
                    warnings.warn(
                        "Mandatory delivery was requested, but "
                        "unroutable messages cannot be detected without "
                        "publish confirms enabled."
                    )
