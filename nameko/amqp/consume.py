from logging import getLogger
from threading import Event

from amqp.exceptions import ConnectionError
from kombu import Connection
from kombu.mixins import ConsumerMixin

_log = getLogger(__name__)


class Consumer(ConsumerMixin):
    """ Helper utility for consuming messages from RabbitMQ.

    If you don't specify `callbacks`, the consumer may be used like an
    iterator?
    """

    def __init__(
        self, amqp_uri, queues=None, callbacks=None, heartbeat=None,
        prefetch_count=None, serializer=None, accept=None, **kwargs
    ):
        self.amqp_uri = amqp_uri

        self.queues = queues
        self.callbacks = callbacks or []  # [self.deque.append]
        self.heartbeat = heartbeat
        self.prefetch_count = prefetch_count
        self.serializer = serializer
        self.accept = accept

        # self.deque = Deque()
        self.ready = Event()

        super(Consumer, self).__init__(**kwargs)

    @property
    def connection(self):
        """ Provide the connection parameters for kombu's ConsumerMixin.

        The `Connection` object is a declaration of connection parameters
        that is lazily evaluated. It doesn't represent an established
        connection to the broker at this point.
        """
        return Connection(self.amqp_uri, heartbeat=self.heartbeat)

    def wait_until_consumer_ready(self):
        """ Wait for initial connection.
        """
        self.ready.wait()

    def get_consumers(self, consumer_cls, channel):
        """ Kombu callback to set up consumers.

        Called after any (re)connection to the broker.
        """
        consumer = consumer_cls(
            queues=self.queues,
            callbacks=self.callbacks,
            accept=self.accept
        )
        consumer.qos(prefetch_count=self.prefetch_count)
        return [consumer]

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        """ Kombu callback when consumers are ready to accept messages.

        Called after any (re)connection to the broker.
        """
        # if this is the very first connection, send the event so anything
        # blocking on this Consumer "starting" can continue
        if not self.ready.is_set():
            self.ready.set()

    def on_connection_error(self, exc, interval):
        _log.warning(
            "Error connecting to broker at {} ({}).\n"
            "Retrying in {} seconds.".format(self.amqp_uri, exc, interval)
        )

    def ack_message(self, message):
        # only attempt to ack if the message connection is alive;
        # otherwise the message will already have been reclaimed by the broker
        if message.channel.connection:
            try:
                message.ack()
            except ConnectionError:  # pragma: no cover
                pass  # ignore connection closing inside conditional

    def requeue_message(self, message):
        # only attempt to requeue if the message connection is alive;
        # otherwise the message will already have been reclaimed by the broker
        if message.channel.connection:
            try:
                message.requeue()
            except ConnectionError:  # pragma: no cover
                pass  # ignore connection closing inside conditional
