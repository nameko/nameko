import pytest
from mock import ANY, Mock

from nameko.rpc import Responder


# python version compat
EXCEPTION_MODULE = Exception.__module__


@pytest.yield_fixture
def unserializable():
    def unserializable_inner():
        pass  # pragma: no cover
    yield unserializable_inner


@pytest.fixture
def message():
    message = Mock()
    message.properties = {'reply_to': '', 'content_type': 'application/json'}
    return message


def test_responder(message, mock_producer):

    exchange = Mock()

    responder = Responder('amqp://localhost', exchange, 'json', message)

    # serialisable result
    result, exc_info = responder.send_response(True, None)
    assert result is True
    assert exc_info is None

    expected_msg = {
        'result': True,
        'error': None
    }
    (msg,), _ = mock_producer.publish.call_args
    assert msg == expected_msg


def test_responder_worker_exc(message, mock_producer):

    exchange = Mock()

    responder = Responder('amqp://localhost', exchange, 'json', message)

    # serialisable exception
    worker_exc = Exception('error')
    result, exc_info = responder.send_response(
        None, (Exception, worker_exc, "tb"))
    assert result is None
    assert exc_info == (Exception, worker_exc, "tb")

    expected_msg = {
        'result': None,
        'error': {
            'exc_path': '{}.Exception'.format(EXCEPTION_MODULE),
            'value': 'error',
            'exc_type': 'Exception',
            'exc_args': ['error']
        }
    }
    (msg,), _ = mock_producer.publish.call_args
    assert msg == expected_msg


@pytest.mark.parametrize("serializer,content_type,exception_info_string", [
    ('json', 'application/json', "is not JSON serializable"),
    ('pickle', 'application/x-python-serialize', "Can't pickle")])
def test_responder_unserializable_result(
        message, mock_producer, unserializable,
        serializer, content_type, exception_info_string):

    message.properties['content_type'] = content_type

    exchange = Mock()

    responder = Responder('amqp://localhost', exchange, serializer, message)

    # unserialisable result
    worker_result = unserializable
    result, exc_info = responder.send_response(worker_result, None)

    # responder will return the error from the serializer
    assert result is None
    # Different kombu versions return different exceptions, so
    # testing for the concrete exception is not feasible
    assert exc_info == (ANY, ANY, ANY)
    assert exception_info_string in str(exc_info[1])

    # and publish a dictionary-serialized UnserializableValueError
    # on worker_result
    expected_msg = {
        'result': None,
        'error': {
            'exc_path': 'nameko.exceptions.UnserializableValueError',
            'value': 'Unserializable value: `{}`'.format(worker_result),
            'exc_type': 'UnserializableValueError',
            'exc_args': [],
        }
    }
    (msg,), _ = mock_producer.publish.call_args
    assert msg == expected_msg


def test_responder_cannot_unicode_exc(message, mock_producer):

    exchange = Mock()

    responder = Responder('amqp://localhost', exchange, 'json', message)

    class CannotUnicode(object):
        def __str__(self):
            raise Exception('error')

    # un-unicode-able exception
    worker_exc = Exception(CannotUnicode())

    # send_response should not throw
    responder.send_response(True, (Exception, worker_exc, "tb"))


def test_responder_cannot_repr_exc(message, mock_producer):

    exchange = Mock()

    responder = Responder('amqp://localhost', exchange, 'json', message)

    class CannotRepr(object):
        def __repr__(self):
            raise Exception('error')

    # un-repr-able exception
    worker_exc = Exception(CannotRepr())

    # send_response should not throw
    responder.send_response(True, (Exception, worker_exc, "tb"))
