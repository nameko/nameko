from logging import getLogger
from threading import Event

from amqp.exceptions import ConnectionError
from kombu import Connection
from kombu.mixins import ConsumerMixin

from nameko.utils import sanitize_url


_log = getLogger(__name__)


class Consumer(ConsumerMixin):
    """ Helper utility for consuming messages from RabbitMQ.
    """

    def __init__(
        self, amqp_uri, ssl=None, queues=None, callbacks=None, heartbeat=None,
        prefetch_count=None, accept=None, **consumer_options
    ):
        self.amqp_uri = amqp_uri
        self.ssl = ssl

        self.queues = queues
        self.callbacks = callbacks or []
        self.heartbeat = heartbeat
        self.prefetch_count = prefetch_count or 0
        self.accept = accept

        self.consumer_options = consumer_options

        self.ready = Event()

        super(Consumer, self).__init__()

    @property
    def connection(self):
        """ Provide the connection parameters for kombu's ConsumerMixin.

        The `Connection` object is a declaration of connection parameters
        that is lazily evaluated. It doesn't represent an established
        connection to the broker at this point.
        """
        return Connection(
            self.amqp_uri, ssl=self.ssl, heartbeat=self.heartbeat
        )

    def stop(self):
        """ Stop this consumer.

        Any messages received between when this method is called and the
        resulting consumer cancel will be requeued.
        """
        self.should_stop = True

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
            callbacks=[self.on_message],
            accept=self.accept,
            **self.consumer_options
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
            "Retrying in {} seconds."
            .format(sanitize_url(self.amqp_uri), exc, interval)
        )

    def on_message(self, body, message):
        if self.should_stop:
            self.requeue_message(message)
            return
        for callback in self.callbacks:
            callback(body, message)

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
