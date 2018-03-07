from textwrap import dedent

from mock import patch


@patch('nameko.cli.main.os')
def test_main(mock_os, tmpdir, capsys, command):

    config = tmpdir.join('config.yaml')
    config.write("""
        FOO: ${FOO:foobar}
        BAR: ${BAR}
    """)

    mock_os.environ = {
        'BAR': '[1,2,3]'
    }

    command(
        'nameko', 'show-config',
        '--config', config.strpath,
    )

    out, _ = capsys.readouterr()

    expected = dedent("""
        BAR:
        - 1
        - 2
        - 3
        FOO: foobar
    """).strip()

    assert out.strip() == expected
