from textwrap import dedent

from mock import patch

from nameko.cli.commands import ShowConfig
from nameko.cli.main import setup_parser, setup_yaml_parser


@patch('nameko.cli.main.os')
def test_main(mock_os, tmpdir, capsys):

    config = tmpdir.join('config.yaml')
    config.write("""
        FOO: ${FOO:foobar}
        BAR: ${BAR}
    """)

    parser = setup_parser()
    setup_yaml_parser()
    args = parser.parse_args([
        'show-config',
        '--config',
        config.strpath,
    ])

    mock_os.environ = {
        'BAR': '[1,2,3]'
    }

    ShowConfig.main(args)
    out, _ = capsys.readouterr()

    expected = dedent("""
        BAR:
        - 1
        - 2
        - 3
        FOO: foobar
    """).strip()

    assert out.strip() == expected
