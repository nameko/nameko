import json

import eventlet
import random

from kombu.common import maybe_declare
from kombu.pools import producers
from kombu import Exchange, Queue, BrokerConnection

from nameko.dependencies import (
    DependencyProvider, dependency_decorator, QueueConsumer, queue_consumers)
from nameko.service import ServiceContainer


ping_exchange = Exchange('ping', durable=False)

LISTEN_TIMEOUT = 5


class Pinger(DependencyProvider):

    def __init__(self):
        self.seqno = random.randint(1, 100)

    def connection_factory(self):
        return BrokerConnection(self.container.config['amqp_uri'])

    def __call__(self):
        with self.connection_factory() as conn:
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


class PingListener(DependencyProvider):

    def _get_queue_consumer(self):
        """ Get or create a QueueConsumer instance for our container
        """
        if not self.container in queue_consumers:
            queue_consumers[self.container] = QueueConsumer(self.container)
        return queue_consumers[self.container]

    def start(self):
        """ Create our queue, register with the QueueConsumer
        """
        queue_consumer = self._get_queue_consumer()
        queue = Queue('ping_queue', exchange=ping_exchange,
                      durable=False, auto_delete=False)
        queue_consumer.register(queue, self.on_message)

    def on_container_started(self):
        queue_consumer = self._get_queue_consumer()
        queue_consumer.start()

    def on_message(self, body, message):
        """ Handle messages from the queue
        """
        args = (body,)
        kwargs = {}
        self.container.spawn(self.name, args, kwargs,
                             callback=lambda _: message.ack())


class PingHandler(DependencyProvider):

    def __init__(self):
        self.results = []

    def call_result(self, method, result):
        self.results.append(result)
        print "call_result", self.results


@dependency_decorator
def ping_listener():
    return PingListener()


class Service(object):

    def __init__(self):
        self.pings = []

    ping = Pinger()
    handler = PingHandler()

    @ping_listener()
    def pong(self, msg):
        payload = json.loads(msg)
        self.pings.append(payload)
        print "ping from {}, seqno {}".format(payload['src'],
                                              payload['seqno'])
        return "pong [seqno={}]".format(payload['seqno'])


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

    # stop the container; forces us to wait for post-processing
    container.stop()

    seqno = service.ping.seqno - 1

    # assert that the service received the ping
    assert service.pings == [{
        "msg": "ping",
        "src": service.ping.name,
        "seqno": seqno
    }]

    # assert that the ping handler received the response
    assert service.handler.results == ["pong [seqno={}]".format(seqno)]



















