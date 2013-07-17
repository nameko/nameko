from kombu import Exchange, Queue
from nameko.common import UIDGEN


DURABLE_QUEUES = False


def get_reply_exchange(msgid, channel=None):
    return Exchange(
        name=msgid,
        channel=channel,
        type='direct',
        durable=False,
        auto_delete=True)


def get_reply_queue(msgid, channel=None):
    exchange = get_reply_exchange(msgid, channel=channel)
    return Queue(
        name=msgid,
        channel=channel,
        exchange=exchange,
        routing_key=msgid,
        durable=False,
        auto_delete=True,
        exclusive=True)


def get_topic_exchange(exchange_name, channel=None):
    return Exchange(
        name=exchange_name,
        channel=channel,
        type='topic',
        durable=DURABLE_QUEUES,
        auto_delete=False)


def get_topic_queue(exchange_name, topic, channel=None):
    exchange = get_topic_exchange(exchange_name, channel=channel)
    return Queue(
        name=topic,
        channel=channel,
        exchange=exchange,
        routing_key=topic,
        durable=DURABLE_QUEUES,
        auto_delete=False,
        exclusive=False)


def get_fanout_exchange(topic, channel=None):
    return Exchange(
        name='{}_fanout'.format(topic),
        channel=channel,
        type='fanout',
        durable=False,
        auto_delete=True)


def get_fanout_queue(topic, channel=None, uidgen=None):
    exchange = get_fanout_exchange(topic, channel=channel)
    unique = uidgen() if uidgen is not None else UIDGEN()
    return Queue(
        name='{}_fanout_{}'.format(topic, unique),
        channel=channel,
        exchange=exchange,
        routing_key=topic,
        durable=False,
        auto_delete=True,
        exclusive=True)
