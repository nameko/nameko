import sys

import pytest
from mock import patch


@pytest.fixture
def command():
    from nameko.cli import cli

    def _command(*argv):
        with patch.object(sys, "argv", list(argv)):
            cli(standalone_mode=False)

    return _command
