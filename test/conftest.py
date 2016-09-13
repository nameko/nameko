import sys

import pytest
from types import ModuleType


@pytest.yield_fixture
def fake_module():
    module = ModuleType("fake_module")
    sys.modules[module.__name__] = module
    yield module
    del sys.modules[module.__name__]
