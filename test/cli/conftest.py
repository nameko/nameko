import sys

import pytest
from mock import patch

from nameko import config
from nameko.cli.main import main


@pytest.fixture
def command():
    config.clear()

    def _command(*argv):
        with patch.object(sys, 'argv', argv):
            main()

    return _command
