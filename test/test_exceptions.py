from mock import patch
import pytest

from nameko.exceptions import (
    serialize, deserialize, deserialize_to_instance, RemoteError)


class CustomError(Exception):
    pass


@pytest.yield_fixture(autouse=True)
def registry():
    with patch('nameko.exceptions.registry', {}) as patched:
        yield patched


def test_serialize():

    exc = CustomError('something went wrong')

    assert serialize(exc) == {
        'exc_type': 'CustomError',
        'exc_path': 'test.test_exceptions.CustomError',
        'value': 'something went wrong',
        'args': ('something went wrong',)
    }


def test_deserialize_to_remote_error():

    exc = CustomError('something went wrong')
    data = serialize(exc)

    deserialized = deserialize(data)
    assert type(deserialized) == RemoteError
    assert deserialized.exc_type == "CustomError"
    assert deserialized.value == "something went wrong"
    assert deserialized.args == ("CustomError something went wrong",)


def test_deserialize_to_instance():

    # register Error as deserializable to an instance
    deserialize_to_instance(CustomError)

    exc = CustomError('something went wrong')
    data = serialize(exc)

    deserialized = deserialize(data)
    assert type(deserialized) == CustomError
    assert deserialized.message == "something went wrong"
    assert deserialized.args == ("something went wrong",)
