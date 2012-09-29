from kombu import Producer

from newrpc import entities
from newrpc.decorators import ensure


@ensure
def reply(connection, msg_id, replydata=None, failure=None, on_return=None):
    if on_return is not None:
        raise NotImplementedError('on_return is not implemented')

    producer_kwargs = {}
    publish_kwargs = {}
    with connection.channel() as channel:
        msg = {'result': replydata, 'failure': failure, 'ending': False, }
        exchange = entities.get_reply_exchange(msg_id)
        producer = Producer(channel,
                exchange=exchange,
                routing_key=msg_id,
                on_return=on_return,
                **producer_kwargs)
        producer.declare()
        producer.publish(msg, **publish_kwargs)
        msg = {'result': None, 'failure': None, 'ending': True, }
        producer.publish(msg, **publish_kwargs)
