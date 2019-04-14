from textwrap import dedent

import pytest
from mock import patch


@patch("nameko.cli.utils.config.os")
@pytest.mark.usefixtures("empty_config")
def test_main(mock_os, tmpdir, capsys, command):

    config_file = tmpdir.join("config.yaml")
    config_file.write(
        """
        FOO: ${FOO:foobar}
        BAR: ${BAR}
    """
    )

    mock_os.environ = {"BAR": "[1,2,3]"}

    command("nameko", "show-config", "--config", config_file.strpath)

    out, _ = capsys.readouterr()

    expected = dedent(
        """
        BAR:
        - 1
        - 2
        - 3
        FOO: foobar
    """
    ).strip()

    assert out.strip() == expected
