from kombu import Exchange, Queue


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
