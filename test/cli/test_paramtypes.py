import sys

import click
import pytest

from nameko.cli.click_paramtypes import (
    HostPortParamType, KeyValParamType, NamekoModuleServicesParamType
)

from test.sample import Service


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


@pytest.mark.parametrize(
    "value,expected",
    [
        ("test.sample", [Service]),
        ("test.sample:Service", [Service]),
        # py2.7 version (skip for py3.x)
        pytest.param(
            "test.missing_module",
            click.exceptions.BadParameter(
                "'test.missing_module'. No module named missing_module"
            ),
            marks=pytest.mark.skipif(
                sys.version_info >= (3, 0),
                reason="This exception texting is py2.x specific",
            ),
        ),
        # py3.x version (skip for py2.x)
        pytest.param(
            "test.missing_module",
            click.exceptions.BadParameter(
                "'test.missing_module'. No module named 'test.missing_module'"
            ),
            marks=pytest.mark.skipif(
                sys.version_info < (3, 0),
                reason="This exception texting is py3.x specific",
            ),
        ),
        pytest.param(
            "test.sample:MissingService",
            click.exceptions.BadParameter(
                "'test.sample:MissingService'. "
                "Failed to find service class 'MissingService' in module 'test.sample'"
            ),
        ),
        pytest.param(
            "test.not_importable",
            click.exceptions.BadParameter("'test.not_importable'. "),
        ),
    ],
)
def test_nameko_module_services(value, expected):
    ctype = NamekoModuleServicesParamType(["."])
    if isinstance(expected, Exception):
        with pytest.raises(expected.__class__) as exc_info:
            result = ctype.convert(value, None, None)
        assert exc_info.value.message == expected.message
    else:
        result = ctype.convert(value, None, None)
        assert result == expected
