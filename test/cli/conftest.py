import sys

import pytest
from mock import patch

from nameko.cli.main import main
from nameko import config


@pytest.fixture
def command():
    config.clear()

    def _command(*argv):
        with patch.object(sys, 'argv', argv):
            main()

    return _command
