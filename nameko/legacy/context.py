import iso8601

from nameko.legacy.common import UTCNOW, UIDGEN


class Context(dict):

    def __init__(self, user_id=None, timestamp=None, request_id=None,
                 **kwargs):

        if isinstance(timestamp, basestring):
            timestamp = iso8601.parse_date(timestamp)

        if user_id is not None:
            self.user_id = user_id
        self.timestamp = timestamp or UTCNOW()
        self.request_id = request_id or UIDGEN()
        self.update(kwargs)

    def __getattr__(self, name):
        if name not in self:
            # backwards compat: user_id attr should exist even if no value set
            if name == "user_id":
                return None
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
        })
        return res

    def add_to_message(self, message):
        return add_context_to_payload(self, message)


def get_admin_context():
    return Context(is_admin=True)


def add_context_to_payload(context, payload):
    for key, value in context.to_dict().iteritems():
        payload['_context_{}'.format(key)] = value
    return payload
