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

    def __init__(self, amqp_uri, serializer):
        self.amqp_uri = amqp_uri
        self.serializer = serializer
        # defaults?

        # we want a solution where:
        # 1. publisher core can be used as a standalone thing
        # 2. options can be defined at class def time, instantiation time, and publish time
        # 3. dependency provider easily wraps the core
        # 4. instantiation time and ideally def time overrides on DP also possible

    @property
    def use_confirms(self):
        """ Enable `confirms <http://www.rabbitmq.com/confirms.html>`_
        for this publisher.

        The publisher will wait for an acknowledgement from the broker that
        the message was receieved and processed appropriately, and otherwise
        raise. Confirms have a performance penalty but guarantee that messages
        aren't lost, for example due to stale connections.
        """
        return True

    @property
    def delivery_options(self):
        return {
            'delivery_mode': PERSISTENT,
            'mandatory': False,
            'priority': 0,
            'expiration': None,
        }

    @property
    def encoding_options(self):
        return {
            'serializer': self.serializer,
            'compression': None
        }

    @property
    def retry(self):
        """ Enable automatic retries when publishing a message that fails due
        to a connection error.

        Retries according to :attr:`self.retry_policy`.
        """
        return True

    @property
    def retry_policy(self):
        """ Policy to apply when retrying message publishes, if enabled.

        See :attr:`self.retry`.
        """
        return DEFAULT_RETRY_POLICY

    def publish(self, propagating_headers, msg, **kwargs):
        """
        """
        exchange = self.exchange
        queue = self.queue

        if exchange is None and queue is not None:
            exchange = queue.exchange

        # add any new headers to the existing ones we're propagating
        headers = propagating_headers.copy()
        headers.update(kwargs.pop('headers', {}))

        retry = kwargs.pop('retry', self.retry)
        retry_policy = kwargs.pop('retry_policy', self.retry_policy)

        for key in self.delivery_options:
            if key not in kwargs:
                kwargs[key] = self.delivery_options[key]
        for key in self.encoding_options:
            if key not in kwargs:
                kwargs[key] = self.encoding_options[key]

        mandatory = kwargs.pop('mandatory', False)

        with get_producer(self.amqp_uri, self.use_confirms) as producer:

            producer.publish(
                msg,
                exchange=exchange,
                headers=headers,
                retry=retry,
                retry_policy=retry_policy,
                mandatory=mandatory,
                **kwargs
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
