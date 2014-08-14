from mock import ANY, Mock, patch
import pytest

from nameko.exceptions import serialize
from nameko.messaging import AMQP_URI_CONFIG_KEY
from nameko.rpc import Responder


@pytest.yield_fixture
def mock_publish():
    path = 'nameko.rpc.producers'
    with patch(path) as patched:
        publish = patched[ANY].acquire().__enter__().publish
        yield publish


def test_responder(mock_publish):

    message = Mock()
    message.properties = {'reply_to': ''}

    container = Mock()
    container.config = {AMQP_URI_CONFIG_KEY: ''}

    responder = Responder(message)

    # serialisable result
    result, exc_info = responder.send_response(container, True, None)
    assert result is True
    assert exc_info is None

    expected_msg = {
        'result': True,
        'error': None
    }
    (msg,), _ = mock_publish.call_args
    assert msg == expected_msg


def test_responder_worker_exc(mock_publish):

    message = Mock()
    message.properties = {'reply_to': ''}

    container = Mock()
    container.config = {AMQP_URI_CONFIG_KEY: ''}

    responder = Responder(message)

    # serialisable exception
    worker_exc = Exception('error')
    result, exc_info = responder.send_response(
        container, None, (Exception, worker_exc, "tb"))
    assert result is None
    assert exc_info == (Exception, worker_exc, "tb")

    expected_msg = {
        'result': None,
        'error': {
            'exc_path': 'exceptions.Exception',
            'value': 'error',
            'exc_type': 'Exception',
            'exc_args': ('error',)
        }
    }
    (msg,), _ = mock_publish.call_args
    assert msg == expected_msg


def test_responder_unserializable_result(mock_publish):

    message = Mock()
    message.properties = {'reply_to': ''}

    container = Mock()
    container.config = {AMQP_URI_CONFIG_KEY: ''}

    responder = Responder(message)

    # unserialisable result
    worker_result = object()
    result, exc_info = responder.send_response(container, worker_result, None)

    # responder will return the TypeError from json.dumps
    assert result is None
    assert exc_info == (TypeError, ANY, ANY)
    assert exc_info[1].message == ("{} is not JSON "
                                   "serializable".format(worker_result))

    # and publish a dictionary-serialized UnserializableValueError
    # on worker_result
    expected_msg = {
        'result': None,
        'error': {
            'exc_path': 'nameko.exceptions.UnserializableValueError',
            'value': 'Unserializable value: `{}`'.format(worker_result),
            'exc_type': 'UnserializableValueError',
            'exc_args': ()
        }
    }
    (msg,), _ = mock_publish.call_args
    assert msg == expected_msg


def test_responder_unserializable_exc(mock_publish):

    message = Mock()
    message.properties = {'reply_to': ''}

    container = Mock()
    container.config = {AMQP_URI_CONFIG_KEY: ''}

    responder = Responder(message)

    # unserialisable exception
    worker_exc = Exception(object())
    result, exc_info = responder.send_response(
        container, True, (Exception, worker_exc, "tb"))

    # responder will return the TypeError from json.dumps
    assert result is None
    assert exc_info == (TypeError, ANY, ANY)
    assert exc_info[1].message == ("{} is not JSON "
                                   "serializable".format(worker_exc.args[0]))

    # and publish a dictionary-serialized UnserializableValueError
    # (where the unserialisable value is a dictionary-serialized worker_exc)
    serialized_exc = serialize(worker_exc)
    expected_msg = {
        'result': None,
        'error': {
            'exc_path': 'nameko.exceptions.UnserializableValueError',
            'value': 'Unserializable value: `{}`'.format(serialized_exc),
            'exc_type': 'UnserializableValueError',
            'exc_args': ()
        }
    }
    (msg,), _ = mock_publish.call_args
    assert msg == expected_msg


def test_responder_cannot_unicode_exc(mock_publish):

    message = Mock()
    message.properties = {'reply_to': ''}

    container = Mock()
    container.config = {AMQP_URI_CONFIG_KEY: ''}

    responder = Responder(message)

    class CannotUnicode(object):
        def __str__(self):
            raise Exception('error')

    # un-unicode-able exception
    worker_exc = Exception(CannotUnicode())

    # send_response should not throw
    responder.send_response(container, True, (Exception, worker_exc, "tb"))


def test_responder_cannot_repr_exc(mock_publish):

    message = Mock()
    message.properties = {'reply_to': ''}

    container = Mock()
    container.config = {AMQP_URI_CONFIG_KEY: ''}

    responder = Responder(message)

    class CannotRepr(object):
        def __repr__(self):
            raise Exception('error')

    # un-repr-able exception
    worker_exc = Exception(CannotRepr())

    # send_response should not throw
    responder.send_response(container, True, (Exception, worker_exc, "tb"))
