from contextlib import contextmanager
import warnings

from kombu import Connection
from kombu.pools import connections, producers
from six.moves import queue as Queue

from nameko.constants import DEFAULT_RETRY_POLICY

# delivery_mode
PERSISTENT = 2


class UndeliverableMessage(Exception):
    """ Raised when publisher confirms are enabled and a message could not
    be routed or persisted """
    pass


@contextmanager
def get_connection(amqp_uri):
    conn = Connection(amqp_uri)
    with connections[conn].acquire(block=True) as connection:
        yield connection


@contextmanager
def get_producer(amqp_uri, confirms=True):
    transport_options = {
        'confirm_publish': confirms
    }
    conn = Connection(amqp_uri, transport_options=transport_options)

    with producers[conn].acquire(block=True) as producer:
        yield producer


class Publisher(object):
    """
    """

    use_confirms = True
    """
    Enable `confirms <http://www.rabbitmq.com/confirms.html>`_ for this
    publisher.

    The publisher will wait for an acknowledgement from the broker that
    the message was receieved and processed appropriately, and otherwise
    raise. Confirms have a performance penalty but guarantee that messages
    aren't lost, for example due to stale connections.
    """

    delivery_mode = PERSISTENT
    """
    Default delivery mode for messages published by this Publisher.
    """

    mandatory = False
    """
    """

    priority = 0
    """
    """

    expiration = None
    """
    """

    serializer = None
    """
    """

    compression = None
    """
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

    def __init__(
        self, amqp_uri, use_confirms=None, serializer=None, compression=None,
        delivery_mode=None, mandatory=None, priority=None, expiration=None,
        retry=None, retry_policy=None, **publish_kwargs
    ):
        self.amqp_uri = amqp_uri

        # MYB: accept exchange and/or routing_key here? if not, justify

        # publish confirms
        self.use_confirms = use_confirms or self.use_confirms

        # delivery options
        self.delivery_mode = delivery_mode or self.delivery_mode
        self.mandatory = mandatory or self.mandatory
        self.priority = priority or self.priority
        self.expiration = expiration or self.expiration

        # message options
        self.serializer = serializer or self.serializer
        self.compression = compression or self.compression

        # retry policy
        self.retry = retry or self.retry
        self.retry_policy = retry_policy or self.retry_policy

        # other publish arguments
        self.publish_kwargs = publish_kwargs

    def publish(self, msg, **kwargs):
        """
        """
        # merge headers and extra_headers
        # MYB: needs explicit test?
        headers = kwargs.pop('headers', {}).copy()
        headers.update(kwargs.pop('extra_headers', {}))

        # MYB: needs test
        use_confirms = kwargs.pop('use_confirms', self.use_confirms)

        delivery_mode = kwargs.pop('delivery_mode', self.delivery_mode)
        mandatory = kwargs.pop('mandatory', self.mandatory)
        priority = kwargs.pop('priority', self.priority)
        expiration = kwargs.pop('expiration', self.expiration)
        serializer = kwargs.pop('serializer', self.serializer)
        compression = kwargs.pop('compression', self.compression)
        retry = kwargs.pop('retry', self.retry)
        retry_policy = kwargs.pop('retry_policy', self.retry_policy)

        publish_kwargs = self.publish_kwargs.copy()
        publish_kwargs.update(kwargs)  # publish-time kwargs win

        with get_producer(self.amqp_uri, use_confirms) as producer:

            producer.publish(
                msg,
                headers=headers,
                delivery_mode=delivery_mode,
                mandatory=mandatory,
                priority=priority,
                expiration=expiration,
                compression=compression,
                retry=retry,
                retry_policy=retry_policy,
                serializer=serializer,
                **publish_kwargs
            )

            if mandatory:
                if not self.use_confirms:
                    warnings.warn(
                        "Mandatory delivery was requested, but "
                        "unroutable messages cannot be detected without "
                        "publish confirms enabled."
                    )
                try:
                    returned_messages = producer.channel.returned_messages
                    returned = returned_messages.get_nowait()
                except Queue.Empty:
                    pass
                else:
                    raise UndeliverableMessage(returned)
