import iso8601

from nameko.common import UTCNOW, UIDGEN


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
        self.timestamp = timestamp or UTCNOW()
        self.request_id = request_id or UIDGEN()
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

    def add_to_message(self, message):
        return add_context_to_payload(self, message)


def get_admin_context():
    return Context(user_id=None, is_admin=True)


def parse_message(message_body):
    method = message_body.pop('method')
    args = message_body.pop('args')
    msg_id = message_body.pop('_msg_id', None)
    context_dict = dict((k[9:], message_body.pop(k))
            for k in message_body.keys()
            if k.startswith('_context_'))
    context = Context(**context_dict)
    return msg_id, context, method, args


def add_context_to_payload(context, payload):
    for key, value in context.to_dict().iteritems():
        payload['_context_{}'.format(key)] = value
    return payload
