# The parametrize function is generated, so this doesn't work:
#
#     from pytest.mark import parametrize
#
from tempfile import NamedTemporaryFile
from mock import patch
from path import path
import pytest

parametrize = pytest.mark.parametrize
raises = pytest.raises

from nameko import metadata
from nameko.nameko_doc.main import main


class TestFileUtils(object):
    def test_example_folder_structure(self, example_projects_source):
        assert example_projects_source.exists()
        examples = {d.basename() for d in example_projects_source.dirs()}
        assert set(examples) == {
            'empty',
            'packages',
            'recommended',
            'recommended_with_controller',
            'ast',
            'ast_from_package',
            'nameko_decorator',
        }

    def test_copy_example_project(self, copy_example):
        copied = copy_example('empty')
        assert copied.exists()
        file_contents = {f.basename() for f in copied.files()}
        assert file_contents == {'empty.txt'}


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

    @pytest.yield_fixture
    def config_file(self):
        with NamedTemporaryFile() as f:
            f.write('''
[ast_extract]
class_instructions: service_class
method_instructions: service_method

[service_class]
trigger_names: register_service, service_class

[service_method]
trigger_names: rpc, service_method, RpcProvider
title: Service Method''')
            f.flush()
            yield f.name

    @parametrize('source_arg', ['-s', '--source'])
    @parametrize('output_arg', ['-o', '--output'])
    @parametrize('config_arg', ['-c', '--config'])
    def test_build_empty_project(self, temp_folder_factory, copy_example,
                                 config_file, source_arg, output_arg,
                                 config_arg):
        empty = copy_example('empty')
        output_folder = temp_folder_factory(reserve_only=True)

        main(['nameko_doc', source_arg, empty, output_arg, output_folder,
              config_arg, config_file])

        assert output_folder.exists()
        assert output_folder.files() == []

    def test_build_into_existing_folder(self, temp_folder_factory,
                                        copy_example, config_file):
        empty = copy_example('empty')
        output_folder = temp_folder_factory()

        # Add something in there
        output_folder.joinpath('bye.txt').write_text('bye!')

        assert len(output_folder.files()) == 1

        main(['nameko_doc', '-s', empty, '-o', output_folder,
              '-c', config_file])

        assert output_folder.exists()
        assert output_folder.files() == []

    @pytest.yield_fixture
    def working_temp(self, temp_folder_factory):
        current = path.getcwd()
        working = temp_folder_factory()
        working.cd()
        yield working
        current.cd()

    @pytest.mark.usefixtures('working_temp')
    def test_no_config(self, temp_folder_factory, copy_example):
        empty = copy_example('empty')
        output_folder = temp_folder_factory()

        with patch('nameko.nameko_doc.processor.importlib') as m:
            main(['nameko_doc', '-s', empty, '-o', output_folder,
                  '--extractor', 'foo.Class'])
            mock_cls = m.import_module('foo').Class
            # Called with no config
            mock_cls.assert_called_with(empty, None)

    # TODO Features: Event handler meths, Events
