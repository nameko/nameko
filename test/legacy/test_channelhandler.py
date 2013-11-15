import mock
import pytest

from nameko.legacy.channelhandler import ChannelHandler


def test_ensure():
    conn = mock.Mock()

    handler = ChannelHandler(conn, create_channel=False)

    handler.ensure(handler.close)
    conn.ensure.assert_called_with(
        handler, handler.close,
        errback=handler.on_error, on_revive=handler.revive)

    obj = {}
    fn = lambda: None
    handler.ensure((obj, fn))
    conn.ensure.assert_called_with(
        obj, fn, errback=handler.on_error, on_revive=None)

    with pytest.raises(TypeError):
        handler.ensure(None)
