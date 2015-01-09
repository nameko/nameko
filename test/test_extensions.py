import pytest

from nameko.dependencies import Extension
from nameko.exceptions import ExtensionError


def test_require_super_init():

    class ExampleExtension(Extension):
        def __init__(self):
            pass

    ext = ExampleExtension()

    with pytest.raises(ExtensionError) as exc_info:
        ext.clone()
    assert "super().__init__" in exc_info.value.message
