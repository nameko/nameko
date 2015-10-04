# coding: utf-8

import json

from mock import patch
import pytest
import six

from nameko.exceptions import (
    serialize, safe_for_serialization, deserialize, deserialize_to_instance,
    RemoteError, UnserializableValueError)


OBJECT_REPR = repr(object)


class CustomError(Exception):
    pass


@pytest.yield_fixture
def registry():
    with patch('nameko.exceptions.registry', {}) as patched:
        yield patched


def test_serialize():

    exc = CustomError('something went wrong')

    assert serialize(exc) == {
        'exc_type': 'CustomError',
        'exc_path': 'test.test_exceptions.CustomError',
        'exc_args': ['something went wrong'],
        'value': 'something went wrong',
    }


def test_serialize_cannot_unicode():

    class CannotUnicode(object):
        def __str__(self):
            raise Exception('boom')

    bad_string = CannotUnicode()
    exc = CustomError(bad_string)

    assert serialize(exc) == {
        'exc_type': 'CustomError',
        'exc_path': 'test.test_exceptions.CustomError',
        'exc_args': ['[__unicode__ failed]'],
        'value': '[__unicode__ failed]',
    }


def test_serialize_args():
    cause = Exception('oops')
    exc = CustomError('something went wrong', cause)

    assert json.dumps(serialize(exc))


def test_deserialize_to_remote_error():

    exc = CustomError(u'something went ಠ_ಠ')
    data = serialize(exc)

    deserialized = deserialize(data)
    assert type(deserialized) == RemoteError
    assert deserialized.exc_type == "CustomError"
    assert deserialized.value == u"something went ಠ_ಠ"
    assert six.text_type(deserialized) == u"CustomError something went ಠ_ಠ"


@pytest.mark.usefixtures('registry')
def test_deserialize_to_instance():

    # register Error as deserializable to an instance
    deserialize_to_instance(CustomError)

    exc = CustomError('something went wrong')
    data = serialize(exc)

    deserialized = deserialize(data)
    assert type(deserialized) == CustomError
    assert str(deserialized) == "something went wrong"


def test_exception_name_clash():

    class MethodNotFound(Exception):
        # application exception with clashing name
        pass

    exc = MethodNotFound('application error')
    data = serialize(exc)

    deserialized = deserialize(data)
    assert type(deserialized) == RemoteError
    assert deserialized.exc_type == "MethodNotFound"
    assert deserialized.value == "application error"
    assert str(deserialized) == "MethodNotFound application error"

    from nameko.exceptions import MethodNotFound as NamekoMethodNotFound

    exc = NamekoMethodNotFound('missing')
    data = serialize(exc)

    deserialized = deserialize(data)
    assert type(deserialized) == NamekoMethodNotFound
    assert str(deserialized) == "missing"


def test_serialize_backwards_compat():

    exc = CustomError('something went wrong')
    data = serialize(exc)

    # nameko < 1.5.0 has no ``exc_path`` or ``exc_args`` keys
    del data['exc_path']
    del data['exc_args']

    deserialized = deserialize(data)
    assert type(deserialized) == RemoteError
    assert deserialized.exc_type == "CustomError"
    assert deserialized.value == "something went wrong"
    assert str(deserialized) == "CustomError something went wrong"

    # nameko < 1.1.4 has an extra ``traceback`` key
    data['traceback'] = "traceback string"

    deserialized = deserialize(data)
    assert type(deserialized) == RemoteError
    assert deserialized.exc_type == "CustomError"
    assert deserialized.value == "something went wrong"
    assert str(deserialized) == "CustomError something went wrong"


def test_unserializable_value_error():

    # normal value
    value = "value"
    exc = UnserializableValueError(value)

    assert exc.repr_value == "'value'"
    assert str(exc) == "Unserializable value: `'value'`"

    # un-repr-able value
    class CannotRepr(object):
        def __repr__(self):
            raise Exception('boom')

    bad_value = CannotRepr()
    exc = UnserializableValueError(bad_value)

    assert exc.repr_value == "[__repr__ failed]"
    assert str(exc) == "Unserializable value: `[__repr__ failed]`"


@pytest.mark.parametrize("value, safe_value", [
    ('str', 'str'),
    (object, OBJECT_REPR),
    ([object], [OBJECT_REPR]),
    ({object}, [OBJECT_REPR]),
    ({'foo': 'bar'}, {'foo': 'bar'}),
    ({object: None}, {OBJECT_REPR: 'None'}),
    ({None: object}, {'None': OBJECT_REPR}),
    ((1, 2), ['1', '2']),
    ([1, [2]], ['1', ['2']]),
])
def test_safe_for_serialization(value, safe_value):
    assert safe_for_serialization(value) == safe_value


def test_safe_for_serialization_bad_str():
    class BadStr(object):
        def __str__(self):
            raise Exception('boom')

    obj = BadStr()
    safe = safe_for_serialization(obj)
    assert isinstance(safe, six.string_types)
    assert 'failed' in safe
