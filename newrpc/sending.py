from kombu import Producer

from newrpc import entities
from newrpc.channelhandler import ChannelHandler

__all__ = ['send_direct', 'send_topic', 'send_fanout', ]


def send_direct(connection, directid, data):
    exchange = entities.get_reply_exchange(directid)
    with ChannelHandler(connection) as ch:
        producer = Producer(ch.channel,
                exchange=exchange,
                routing_key=directid)
        ch.ensure(producer.publish)(data, declare=[exchange])


def send_topic(connection, exchange, topic, data):
    exchange = entities.get_topic_exchange(exchange)
    with ChannelHandler(connection) as ch:
        producer = Producer(ch.channel,
                exchange=exchange,
                routing_key=topic)
        ch.ensure(producer.publish)(data, declare=[exchange])


def send_fanout(connection, topic, data):
    exchange = entities.get_fanout_exchange(topic)
    with ChannelHandler(connection) as ch:
        producer = Producer(ch.channel,
                exchange=exchange,
                routing_key=topic)
        ch.ensure(producer.publish)(data, declare=[exchange])
