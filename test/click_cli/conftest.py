import sys

import pytest
from mock import patch


@pytest.fixture
def command():
    from nameko.click_cli.main import main

    def _command(*argv):
        with patch.object(sys, "argv", list(argv)):
            main(standalone_mode=False)

    return _command
