import os
import sys
from importlib import import_module

from nameko.utils import autoreload
from mock import patch, Mock
import pytest
from contextlib import contextmanager


@contextmanager
def temp_extend_syspath(*paths):
    _orig_sys_path = sys.path[:]
    sys.path.extend(paths)
    try:
        yield
    finally:
        sys.path = _orig_sys_path


def test_known_filepaths_are_strings():
    for filename in autoreload._generate_known_filenames():
        assert isinstance(filename, str)


def test_only_include_new_files_then_only_newly_added_files_returned(tmpdir):
    module = 'test_only_include_new_files.py'
    mod = tmpdir.join(module)
    mod.write('')
    filename = f'{tmpdir}/{module}'

    # uncached access check
    _clear_cache()
    filenames = autoreload._generate_known_filenames(
        only_include_new_files=True
    )
    filenames_reference = autoreload._generate_known_filenames()
    assert set(filenames) == set(filenames_reference)

    # cached access check: no changes
    filenames = autoreload._generate_known_filenames(
        only_include_new_files=True
    )
    assert set(filenames) == set()

    # cached access check: add a module
    with temp_extend_syspath(tmpdir):
        import_module(module.replace('.py', ''))
    filenames = autoreload._generate_known_filenames(
        only_include_new_files=True
    )
    assert set(filenames) == {filename}


def test_when_file_deleted_is_no_longer_returned(tmpdir):
    module = 'test_deleted_removed_module.py'
    mod = tmpdir.join(module)
    mod.write('')
    filename = f'{tmpdir}/{module}'

    with temp_extend_syspath(tmpdir):
        import_module('test_deleted_removed_module')
    _check_file_found(filename)

    os.unlink(filename)
    _check_file_not_found(filename)


def test_files_which_raise_are_still_known(tmpdir):
    module = 'test_error.py'
    mod = tmpdir.join(module)
    mod.write('1/0')
    filename = f'{tmpdir}/{module}'

    with temp_extend_syspath(tmpdir):
        with pytest.raises(ZeroDivisionError):
            autoreload._raise_app_errors(import_module)('test_error')
    _check_file_found(filename)


def test_raise_app_errors_only_include_new_files(tmpdir):
    module = 'test_error.py'
    mod = tmpdir.join(module)
    mod.write('1/0')
    filename = f'{tmpdir}/{module}'

    with temp_extend_syspath(tmpdir):
        with pytest.raises(ZeroDivisionError):
            autoreload._raise_app_errors(import_module)('test_error')
    _check_new_file_found(filename)


def test_raise_app_errors_catches_all_exceptions(tmpdir):
    module = 'test_exception.py'
    mod = tmpdir.join(module)
    mod.write('raise Exception')
    filename = f'{tmpdir}/{module}'

    with temp_extend_syspath(tmpdir):
        with pytest.raises(Exception):
            autoreload._raise_app_errors(import_module)('test_exception')
    _check_file_found(filename)


def test_clean_python_files(tmpdir):
    file_paths = [
        tmpdir.join('file1.txt'),
        tmpdir.join('file2.pyo'),
        tmpdir.join('file3.$py.class'),
        tmpdir.join('file4.py'),
    ]
    for filename in file_paths:
        filename.write('')

    tmpdir.join('file3.py').write('')
    tmpdir.join('file2.py').write('')

    expected = [
        os.path.join(tmpdir, 'file1.txt'),
        os.path.join(tmpdir, 'file2.py'),
        os.path.join(tmpdir, 'file4.py'),
    ]
    paths = [str(p) for p in file_paths]
    assert autoreload._clean_python_files(file_paths=paths) == expected


def test_make_autoreload_exists_when_nameko_run_exists():
    with pytest.raises(SystemExit):
        autoreload.make_autoreload(app_run_func=lambda: sys.exit(0))


class TestCodeHasChanged:

    def test_when_file_last_modified_changed_returns_true(self):
        assert autoreload._code_has_changed() is False
        assert len(autoreload.FILES_LAST_MODIFIED_VALUES) > 0

        with patch('nameko.utils.autoreload.os') as mock_os:
            mock_os.stat().st_mtime = 9999.9999
            assert autoreload._code_has_changed() is True


class TestMonitorNeedsReloading:

    @patch('nameko.utils.autoreload._code_has_changed')
    def test_exits_with_reload_exit_code_when_code_changed(
        self, mock_code_has_changed
    ):
        mock_code_has_changed.side_effect = [False, True]
        with pytest.raises(SystemExit) as exc:
            autoreload._monitor_needs_reloading()
        assert exc.value.code == autoreload.RELOAD_EXIT_CODE


class TestRestartApplication:
    @patch('nameko.utils.autoreload.os')
    def test_exists_with_process_code_if_exit_code_not_reload_exit_code(
        self, mock_os
    ):
        mock_os.spawnve.return_value = 1
        assert autoreload._restart_application_with_autoreload() == 1


class TestReloader:

    @patch('nameko.utils.autoreload.os')
    @patch('nameko.utils.autoreload._restart_application_with_autoreload')
    def test_when_env_var_set_and_negative_exit_code_process_killed(
        self, mock_restart_application_with_autoreload, mock_os
    ):
        mock_os.environ = {'RUN_MAIN_APP': 'false'}
        mock_restart_application_with_autoreload.return_value = -1
        autoreload._reloader(Mock(), args=(), kwargs={})
        assert mock_os.kill.call_count == 1

    @patch('nameko.utils.autoreload.os')
    @patch('nameko.utils.autoreload._restart_application_with_autoreload')
    def test_when_env_var_set_and_positive_exit_code_process_exits(
        self, mock_restart_application_with_autoreload, mock_os
    ):
        mock_os.environ = {'RUN_MAIN_APP': 'false'}
        mock_restart_application_with_autoreload.return_value = 1
        with pytest.raises(SystemExit) as exc:
            autoreload._reloader(Mock(), args=(), kwargs={})
        assert exc.value.code == 1

    @patch('nameko.utils.autoreload._monitor_needs_reloading')
    @patch('nameko.utils.autoreload.os')
    @patch('nameko.utils.autoreload.thread')
    def test_when_env_var_not_set_app_is_ran_in_new_thread(
        self, mock_thread, mock_os, mock_monitor_needs_reloading
    ):
        mock_os.environ = {'RUN_MAIN_APP': 'true'}
        autoreload._reloader(Mock(), args=(), kwargs={})
        assert mock_thread.start_new_thread.call_count == 1


def _clear_cache():
    autoreload.CACHED_MODULES = set()
    autoreload.CACHED_FILENAMES = []


def _check_file_found(filename):
    _clear_cache()
    # uncached access check
    assert filename in autoreload._generate_known_filenames()
    # cached access check
    assert filename in autoreload._generate_known_filenames()


def _check_file_not_found(filename):
    _clear_cache()
    # uncached access check
    assert filename not in autoreload._generate_known_filenames()
    # cached access check
    assert filename not in autoreload._generate_known_filenames()


def _check_new_file_found(filename):
    _clear_cache()
    # uncached access check
    assert filename in autoreload._generate_known_filenames(
        only_include_new_files=True
    )
    # cached access check
    assert filename not in autoreload._generate_known_filenames(
        only_include_new_files=True
    )
