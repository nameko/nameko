from logging import getLogger
from threading import Event

from kombu import Connection
from kombu.mixins import ConsumerMixin

from nameko.constants import (
    AMQP_URI_CONFIG_KEY, DEFAULT_HEARTBEAT, DEFAULT_SERIALIZER,
    HEARTBEAT_CONFIG_KEY, SERIALIZER_CONFIG_KEY, PREFETCH_COUNT_CONFIG_KEY,
    DEFAULT_PREFETCH_COUNT
)


_log = getLogger(__name__)


class Consumer(ConsumerMixin):
    """ Helper utility for consuming messages from RabbitMQ.

    Can be used as a mixin or instantiated directly.
    """

    def __init__(self, config=None, queues=None, callbacks=None, **kwargs):
        self._config = config
        self._queues = queues

        self.callbacks = callbacks or []
        self.ready = Event()

        super(ConsumerMixin, self).__init__(**kwargs)

    @property
    def config(self):
        """ We need to use a property and setter for config because some
        subclasses don't have config until they're bound to a container.
        TODO Remove this once https://github.com/nameko/nameko/pull/520 lands
        """
        return self._config or {AMQP_URI_CONFIG_KEY: ''}

    @config.setter
    def config(self, value):
        self._config = value

    @property
    def queues(self):
        """ We need to use a property and setter for queues because some
        subclasses don't define queues until the service name is known.
        """
        return self._queues or []

    @queues.setter
    def queues(self, value):
        self._queues = value

    @property
    def amqp_uri(self):
        return self.config[AMQP_URI_CONFIG_KEY]

    @property
    def prefetch_count(self):
        return self.config.get(
            PREFETCH_COUNT_CONFIG_KEY, DEFAULT_PREFETCH_COUNT
        )

    @property
    def serializer(self):
        return self.config.get(
            SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER
        )

    @property
    def accept(self):
        return self.serializer

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
            accept=[self.serializer]
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
