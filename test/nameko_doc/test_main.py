# The parametrize function is generated, so this doesn't work:
#
#     from pytest.mark import parametrize
#
import pytest

parametrize = pytest.mark.parametrize
raises = pytest.raises

from nameko import metadata
from nameko.nameko_doc.main import main


EMPTY_LOADER_FN = 'test.nameko_doc.test_main.empty_loader'


def empty_loader():
    return []


class TestMain(object):
    @parametrize('helparg', ['-h', '--help'])
    def test_help(self, helparg, capsys):
        with raises(SystemExit) as exc_info:
            main(['nameko_doc', helparg])
        out, err = capsys.readouterr()
        # Should have printed some sort of usage message. We don't
        # need to explicitly test the content of the message.
        assert 'usage' in out
        # Should have used the program name from the argument
        # vector.
        assert 'nameko_doc' in out
        # Should exit with zero return code.
        assert exc_info.value.code == 0

    @parametrize('versionarg', ['-V', '--version'])
    def test_version(self, versionarg, capsys):
        with raises(SystemExit) as exc_info:
            main(['nameko_doc', versionarg])
        out, err = capsys.readouterr()
        # Should print out version.
        project_for_cli = 'Nameko Service Doc'
        assert err == '{0} {1}\n'.format(project_for_cli, metadata.version)
        # Should exit with zero return code.
        assert exc_info.value.code == 0

    @parametrize('output_arg', ['-o', '--output'])
    @parametrize('loader_arg', ['-s', '--service-loader'])
    def test_build_empty_project(self, temp_folder_factory, output_arg,
                                 loader_arg):
        output_folder = temp_folder_factory(reserve_only=True)

        main(['nameko_doc',  output_arg, output_folder, loader_arg,
              EMPTY_LOADER_FN])

        assert output_folder.exists()
        assert output_folder.files() == []

    def test_build_into_existing_folder(self, temp_folder_factory):
        output_folder = temp_folder_factory()

        # Add something in there
        output_folder.joinpath('bye.txt').write_text('bye!')

        assert len(output_folder.files()) == 1

        main(['nameko_doc', '-s', EMPTY_LOADER_FN, '-o', output_folder])

        assert output_folder.exists()
        assert output_folder.files() == []
