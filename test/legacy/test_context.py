import unittest

from nameko.legacy import context
from nameko.legacy.nova import parse_message


class TestContext(unittest.TestCase):

    def _makeOne(self, *args, **kwargs):
        return context.Context(*args, **kwargs)

    def test_constructor_defaults(self):
        user_id = 1
        ctx = self._makeOne(user_id)

        assert ctx.user_id == ctx['user_id'] == user_id

        assert ctx.timestamp == ctx['timestamp']
        assert ctx.timestamp is not None

        assert ctx.request_id == ctx['request_id']
        assert ctx.request_id is not None

    def test_custom_constructor_args(self):
        user_id = 1
        ctx = self._makeOne(user_id, custom='foobar')

        assert ctx.user_id == ctx['user_id'] == user_id

        assert ctx.custom == ctx['custom'] == 'foobar'

    def test_repr(self):
        user_id = 1
        ctx = self._makeOne(user_id)

        assert repr(ctx) == 'Context({})'.format(ctx.to_dict())

    def test_attribute_behaviour(self):
        user_id = 1
        ctx = self._makeOne(user_id)

        with self.assertRaises(AttributeError):
            ctx.custom

        ctx.custom = 'foobar'
        assert ctx.custom == 'foobar'

        del ctx.custom

        with self.assertRaises(AttributeError):
            ctx.custom

    def test_dict_behaviour(self):
        user_id = 1
        ctx = self._makeOne(user_id)

        assert 'custom' not in ctx
        assert ctx.get('custom') is None
        with self.assertRaises(KeyError):
            ctx['custom']

        ctx['custom'] = 'foobar'

        assert 'custom' in ctx
        assert ctx.get('custom') == 'foobar'
        assert ctx['custom'] == 'foobar'

        del ctx['custom']

        assert 'custom' not in ctx
        assert ctx.get('custom') is None
        with self.assertRaises(KeyError):
            ctx['custom']

    def test_to_dict(self):
        """ context should serialise the timestamp for transport
        """
        from datetime import datetime

        user_id = 1
        ctx = self._makeOne(user_id)

        assert isinstance(ctx['timestamp'], datetime)

        serialised = ctx.to_dict()

        assert isinstance(serialised['timestamp'], str)

    def test_transport(self):
        user_id = 1
        ctx = self._makeOne(user_id)

        message_base = {
            'method': 'rpc_call',
            'args': {'arg1': 'val1'},
            '_msg_id': ctx.request_id,
        }
        message = message_base.copy()

        ctx.add_to_message(message)

        msg_id, new_ctx, method, args = parse_message(message)

        assert msg_id == message_base['_msg_id']
        assert id(ctx) != id(new_ctx)
        assert ctx.to_dict() == new_ctx.to_dict()
        assert method == message_base['method']
        assert args == message_base['args']
