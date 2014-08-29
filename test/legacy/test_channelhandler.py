import mock
import pytest

from nameko.legacy.channelhandler import ChannelHandler


def test_ensure():
    conn = mock.Mock()

    handler = ChannelHandler(conn, create_channel=False)

    handler.ensure(handler.close)
    conn.ensure.assert_called_with(
        handler, handler.close, on_revive=handler.revive)

    obj = {}
    fn = lambda: None
    handler.ensure((obj, fn))
    conn.ensure.assert_called_with(obj, fn, on_revive=None)

    with pytest.raises(TypeError):
        handler.ensure(None)
