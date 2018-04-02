import os
from distutils import spawn

import pytest


def toxiproxy_is_installed():
    return spawn.find_executable('toxiproxy-server') is not None


def on_travis():
    return os.environ.get('TRAVIS', False)


def skip_toxiproxy():
    if not on_travis():
        return not toxiproxy_is_installed()


skip_if_no_toxiproxy = pytest.mark.skipif(
    skip_toxiproxy(), reason="toxiproxy not installed"
)
