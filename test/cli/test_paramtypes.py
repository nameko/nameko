import click
import pytest

from nameko.cli.click_paramtypes import HostPortParamType, KeyValParamType


@pytest.mark.parametrize(
    "value,expected", [
        ("KEY=value", ("KEY", "value")),
        ("FLAG", ("FLAG", True)),
        (3.14, click.exceptions.BadParameter("Value must by a string"))
    ]
)
def test_key_value(value, expected):
    ctype = KeyValParamType()
    if isinstance(expected, Exception):
        with pytest.raises(expected.__class__) as exc_info:
            result = ctype.convert(value, None, None)
        assert exc_info.value.message == expected.message
    else:
        result = ctype.convert(value, None, None)
        assert result == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("9999", ("localhost", 9999)),
        ("localhost:9999", ("localhost", 9999)),
        ("host:9999", ("host", 9999)),
        pytest.param(
            "9999a", click.exceptions.BadParameter("9999a is not a valid port number")
        ),
    ],
)
def test_host_port(value, expected):
    ctype = HostPortParamType()
    if isinstance(expected, Exception):
        with pytest.raises(expected.__class__) as exc_info:
            result = ctype.convert(value, None, None)
        assert exc_info.value.message == expected.message
    else:
        result = ctype.convert(value, None, None)
        assert result == expected
