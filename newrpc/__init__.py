import datetime
import uuid

import eventlet
import iso8601
from kombu import Exchange, Producer, Queue

from newrpc.consuming import consumefrom

UTC = iso8601.iso8601.UTC
DURABLE_QUEUES = False
DEFAULT_RPC_TIMEOUT = 10


class WaiterTimeout(Exception):
    pass


class RemoteError(Exception):
    def __init__(self, exc_type=None, value=None, traceback=None):
        self.exc_type = exc_type
        self.value = value
        self.traceback = traceback
        message = 'Remote error: {} {}\n{}'.format(exc_type, value, traceback)
        super(RemoteError, self).__init__(message)


def ifirst(iter_):
    for i in iter_:
        return i


def first(iter_):
    ret = ifirst(iter_)
    for i in iter_:
        i.ack()
    return ret


class Context(object):
    def __init__(self, user_id, is_admin=None, roles=None,
            remote_address=None, timestamp=None,
            request_id=None, auth_token=None, **kwargs):
        # we currently care little about what goes in here
        # from nova's context
        self.user_id = user_id
        self.is_admin = is_admin or False
        self.roles = roles or []
        self.remote_address = remote_address
        if isinstance(timestamp, basestring):
            timestamp = iso8601.parse_date(timestamp)
        self.timestamp = timestamp or datetime.datetime.now(UTC)
        self.request_id = request_id or uuid.uuid4().hex
        self.auth_token = auth_token
        self.extra_kwargs = kwargs

    def to_dict(self):
        # TODO: convert to UTC if not?
        timestamp = self.timestamp.isoformat()
        timestamp = timestamp[:timestamp.index('+')]
        return {'user_id': self.user_id,
                'is_admin': self.is_admin,
                'roles': self.roles,
                'remote_address': self.remote_address,
                'timestamp': timestamp,
                'request_id': self.request_id,
                'auth_token': self.auth_token,
                'project_id': None, }


def get_admin_context():
    return Context(user_id=None, is_admin=True)


def parse_message(message_body):
    method = message_body.pop('method')
    args = message_body.pop('args')
    msg_id = message_body.pop('_msg_id')
    context_dict = dict((k[9:], message_body.pop(k))
            for k in message_body.keys()
            if k.startswith('_context_'))
    context = Context(**context_dict)
    return msg_id, context, method, args


def add_context_to_payload(context, payload):
    for key, value in context.to_dict().iteritems():
        payload['_context_{}'.format(key)] = value
    return payload


def create_rpcpayload(context, method, args, msg_id=None):
    message = {'method': method, 'args': args, }
    message = add_context_to_payload(context, message)
    if msg_id is None:
        msg_id = uuid.uuid4().hex
    if msg_id is not False:
        message['_msg_id'] = msg_id
    return msg_id, message


def get_reply_exchange(msgid, channel=None):
    return Exchange(name=msgid,
            channel=channel,
            type='direct',
            durable=False,
            auto_delete=True)


def get_reply_queue(msgid, channel=None):
    exchange = get_reply_exchange(msgid, channel=channel)
    return Queue(name=msgid,
            channel=channel,
            exchange=exchange,
            routing_key=msgid,
            durable=False,
            auto_delete=True,
            exclusive=True)


def get_topic_exchange(exchange_name, channel=None):
    return Exchange(name=exchange_name,
            channel=channel,
            type='topic',
            durable=DURABLE_QUEUES,
            auto_delete=False)


def get_topic_queue(exchange_name, topic, channel=None):
    exchange = get_topic_exchange(exchange_name, channel=channel)
    return Queue(name=topic,
            channel=channel,
            exchange=exchange,
            routing_key=topic,
            durable=DURABLE_QUEUES,
            auto_delete=False,
            exclusive=False)


def get_fanout_exchange(topic, channel=None):
    return Exchange(name='{}_fanout'.format(topic),
            channel=channel,
            type='fanout',
            durable=False,
            auto_delete=True)


def get_fanout_queue(topic, channel=None, uidgen=None):
    exchange = get_fanout_exchange(topic, channel=channel)
    unique = uidgen() if uidgen is not None else uuid.uuid4().hex
    return Queue(name='{}_fanout_{}'.format(topic, unique),
            channel=channel,
            exchange=exchange,
            routing_key=topic,
            durable=False,
            auto_delete=True,
            exclusive=True)


def send_direct(channel, directid, data):
    exchange = get_reply_exchange(directid)
    producer = Producer(channel,
            exchange=exchange,
            routing_key=directid)
    producer.declare()
    producer.publish(data)


def send_topic(channel, exchange, topic, data):
    exchange = get_topic_exchange(exchange, topic)
    producer = Producer(channel,
            exchange=exchange,
            routing_key=topic)
    producer.declare()
    producer.publish(data)


def send_fanout(channel, topic, data):
    exchange = get_fanout_exchange(topic)
    producer = Producer(channel,
            exchange=exchange,
            routing_key=topic)
    producer.declare()
    producer.publish(data)


def queue_waiter(queue, channel=None, no_ack=False, timeout=None):
    if queue.is_bound:
        if channel is not None:
            raise TypeError('channel specified when queue is bound')
        channel = queue.channel
    elif channel is not None:
        queue.bind(channel)
    else:
        raise TypeError('channel can not be None for unbound queue')
    channel = queue.channel
    buf = []

    def callback(message):
        try:
            message = channel.message_to_python(message)
        except AttributeError:
            pass
        buf.append(message)

    tag = queue.consume(callback=callback, no_ack=no_ack)
    with eventlet.Timeout(timeout, exception=WaiterTimeout()):
        try:
            while True:
                if buf:
                    yield buf.pop(0)
                consumefrom(channel.connection.client)
        finally:
            queue.cancel(tag)


def iter_rpcresponses(queue, channel=None, timeout=None, **kwargs):
    qw = queue_waiter(queue, channel=channel, timeout=timeout, **kwargs)
    for msg in qw:
        data = msg.payload
        if data['failure']:
            raise RemoteError(**msg)
        elif data.get('ending', False):
            msg.ack()
            return
        else:
            yield msg


def send_rpc(context, channel, exchange, topic, method, args,
        timeout=DEFAULT_RPC_TIMEOUT):
    msgid, payload = create_rpcpayload(context, method, args)
    queue = get_reply_queue(msgid, channel=channel)
    queue.declare()
    send_topic(channel, exchange, topic, payload)
    return first(iter_rpcresponses(queue, timeout=timeout))


def reply(channel, msg_id, replydata=None, failure=None, on_return=None):
    if on_return is not None:
        raise NotImplementedError('on_return is not implemented')

    producer_kwargs = {}
    publish_kwargs = {}
    connection = None
    try:
        # TODO: implement results where ending != True (ie: iterator results)
        msg = {'result': replydata, 'failure': failure, 'ending': False, }
        exchange = get_reply_exchange(msg_id)
        producer = Producer(channel,
                exchange=exchange,
                routing_key=msg_id,
                on_return=on_return,
                **producer_kwargs)
        producer.declare()
        producer.publish(msg, **publish_kwargs)
        msg = {'result': None, 'failure': None, 'ending': True, }
        producer.publish(msg, **publish_kwargs)
    finally:
        if connection:
            connection.release()
