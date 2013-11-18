import iso8601

from nameko.legacy.common import UTCNOW, UIDGEN


class Context(dict):

    def __init__(self, user_id, timestamp=None, request_id=None, **kwargs):
        if isinstance(timestamp, basestring):
            timestamp = iso8601.parse_date(timestamp)
        self.timestamp = timestamp or UTCNOW()
        self.request_id = request_id or UIDGEN()
        self.update(kwargs)

        # we currently care little about what goes in here
        # from nova's context
        self.user_id = user_id

    def __getattr__(self, name):
        if name not in self:
            raise AttributeError("Context has no attribute '{}'".format(name))
        return self[name]

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        del self[name]

    def __repr__(self):
        return 'Context({})'.format(self.to_dict())

    def to_dict(self):
        # TODO: convert to UTC if not?
        res = self.copy()

        timestamp = self.timestamp.isoformat()
        timestamp = timestamp[:timestamp.index('+')]
        res.update({
            'timestamp': timestamp,
            'project_id': None,
        })
        return res

    def add_to_message(self, message):
        return add_context_to_payload(self, message)


def get_admin_context():
    return Context(user_id=None, is_admin=True)


def add_context_to_payload(context, payload):
    for key, value in context.to_dict().iteritems():
        payload['_context_{}'.format(key)] = value
    return payload
