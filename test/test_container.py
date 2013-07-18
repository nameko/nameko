import json

import eventlet

from kombu.common import maybe_declare
from kombu.pools import producers
from kombu import Exchange, Queue
from kombu.mixins import ConsumerMixin

from nameko.dependencies import DependencyProvider, dependency_decorator
from nameko.service import ServiceContainer


ping_exchange = Exchange('ping', durable=False)

LISTEN_TIMEOUT = 5


class Pinger(DependencyProvider):

    def __init__(self):
        self.seqno = 1

    def __call__(self):
        with self.container.connection_factory() as conn:
            with producers[conn].acquire(block=True) as producer:
                channel = producer.channel

                maybe_declare(ping_exchange, channel)

                msg = json.dumps({
                    "msg": "ping",
                    "src": self.name,
                    "seqno": self.seqno
                })
                producer.publish(msg, exchange=ping_exchange)

        self.seqno += 1


class PingListener(ConsumerMixin, DependencyProvider):

    queue = None
    consumer = None
    greenlet = None

    def get_consumers(self, Consumer, channel):
        self.consumer = Consumer(queues=[self.queue],
                                 callbacks=[self.on_message])
        return [self.consumer]

    def container_starting(self):
        """ Create our queue
        """
        self.connection = self.container.connection_factory()
        self.queue = Queue('ping_queue', exchange=ping_exchange,
                           durable=False, auto_delete=True)
        self.greenlet = eventlet.spawn(self.run)

        # self.run still isn't ready unless we sleep for a bit
        eventlet.sleep(1)

    def container_stopping(self):
        """ Gracefully shut down
        """
        if self.consumer:
            self.consumer.cancel()
        self.connection.close()

    def on_message(self, body, message):
        """ Handle messages from the queue
        """
        print "on message", body
        args = (body,)
        kwargs = {}

        def ack(result):
            print "message.ack", result

        self.container.dispatch(self.name, args, kwargs, callback=ack)


@dependency_decorator
def ping_listener():
    return PingListener()


class Service(object):

    def __init__(self):
        self.pings = []

    ping = Pinger()

    @ping_listener()
    def pong(self, msg):
        payload = json.loads(msg)
        self.pings.append(payload)
        print "ping from {}, seqno {}".format(payload['src'],
                                              payload['seqno'])
        return "pong to seqno {}".format(payload['seqno'])


def test_pings():

    config = {
        'amqp_uri': 'amqp://guest:guest@localhost/ofsplatform'
    }
    service = Service()
    container = ServiceContainer(service, config)

    container.start()

    # send a ping
    service.ping()

    # wait for it
    with eventlet.timeout.Timeout(LISTEN_TIMEOUT):
        while not service.pings:
            eventlet.sleep()

    assert service.pings == [{
        "msg": "ping",
        "src": service.ping.name,
        "seqno": 1
    }]

    container.stop()
