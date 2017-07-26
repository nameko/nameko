import os
import sys
from importlib import import_module
from contextlib import contextmanager

import pytest
from nameko.constants import AMQP_URI_CONFIG_KEY
from nameko.cli.run import run
from nameko.utils import autoreload
from mock import patch, Mock


config = {AMQP_URI_CONFIG_KEY: 'pyamqp://guest:guest@localhost'}


@patch('nameko.utils.autoreload._reloader')
def test_make_autoreload(mock_reloader):
    autoreload.make_autoreload(
        run,
        args=([Mock()], config),
        kwargs={'backdoor_port': None}
    )
    assert mock_reloader.call_count == 1


def test_make_autoreload_exists_when_nameko_run_exists():
    with pytest.raises(SystemExit):
        autoreload.make_autoreload(app_run_func=lambda: sys.exit(0))


@patch('nameko.utils.autoreload.os')
@patch('nameko.utils.autoreload.sys')
class TestReloader:

    @patch('nameko.utils.autoreload._restart_application_with_autoreload')
    def test_kills_process_if_env_not_true_and_non_zero_exit_code(
        self, mock_restart, mock_sys, mock_os
    ):
        mock_os.environ = {'RUN_MAIN_APP': 'false'}
        mock_restart.return_value = -1
        autoreload._reloader(
            Mock(),
            args=([Mock()], config),
            kwargs={'backdoor_port': None}
        )
        mock_os.kill.assert_called_once_with(mock_os.getpid(), 1)

    @patch('nameko.utils.autoreload._restart_application_with_autoreload')
    def test_exits_process_if_env_not_true_and_non_zero_exit_code(
        self, mock_restart, mock_sys, mock_os
    ):
        mock_os.environ = {'RUN_MAIN_APP': 'false'}
        mock_restart.return_value = 0
        autoreload._reloader(
            Mock(),
            args=([Mock()], config),
            kwargs={'backdoor_port': None}
        )
        mock_sys.exit.assert_called_once_with(0)

    @patch('nameko.utils.autoreload._monitor_needs_reloading')
    def test_if_env_true_app_is_ran_with_reload_monitor(
        self, mock_monitor, mock_sys, mock_os
    ):
        mock_os.environ = {'RUN_MAIN_APP': 'true'}
        autoreload._reloader(
            Mock(),
            args=([Mock()], config),
            kwargs={'backdoor_port': None}
        )
        assert mock_monitor.call_count == 1


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
    filename = '{}/{}'.format(tmpdir, module)

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
    with temp_extend_syspath(str(tmpdir)):
        import_module(module.replace('.py', ''))
    filenames = autoreload._generate_known_filenames(
        only_include_new_files=True
    )
    assert set(filenames) == {filename}


def test_when_file_deleted_is_no_longer_returned(tmpdir):
    module = 'test_deleted_removed_module.py'
    mod = tmpdir.join(module)
    mod.write('')
    filename = '{}/{}'.format(tmpdir, module)

    with temp_extend_syspath(str(tmpdir)):
        import_module('test_deleted_removed_module')
    _check_file_found(filename)

    os.unlink(filename)
    _check_file_not_found(filename)


def test_files_which_raise_are_still_known(tmpdir):
    module = 'test_error.py'
    mod = tmpdir.join(module)
    mod.write('1/0')
    filename = '{}/{}'.format(tmpdir, module)

    with temp_extend_syspath(str(tmpdir)):
        with pytest.raises(ZeroDivisionError):
            autoreload._raise_app_errors(import_module)('test_error')
    _check_file_found(filename)


def test_raise_app_errors_only_include_new_files(tmpdir):
    module = 'test_error.py'
    mod = tmpdir.join(module)
    mod.write('1/0')
    filename = '{}/{}'.format(tmpdir, module)

    with temp_extend_syspath(str(tmpdir)):
        with pytest.raises(ZeroDivisionError):
            autoreload._raise_app_errors(import_module)('test_error')
    _check_new_file_found(filename)


def test_raise_app_errors_catches_all_exceptions(tmpdir):
    module = 'test_exception.py'
    mod = tmpdir.join(module)
    mod.write('raise Exception')
    filename = '{}/{}'.format(tmpdir, module)

    with temp_extend_syspath(str(tmpdir)):
        with pytest.raises(Exception):
            autoreload._raise_app_errors(import_module)('test_exception')
    _check_file_found(filename)


@patch('nameko.utils.autoreload.os')
def test_clean_python_files(mock_os):
    mock_os.path.exists.return_value = True
    file_paths = [
        'file1.txt',
        'file2.pyo',
        'file3$py.class',
        'file4.py',
    ]
    expected = [
        'file1.txt',
        'file2.py',
        'file3.py',
        'file4.py',
    ]
    paths = [str(p) for p in file_paths]
    assert autoreload._clean_python_files(file_paths=paths) == expected


class TestCodeHasChanged:

    @patch('nameko.utils.autoreload.os')
    @patch('nameko.utils.autoreload._generate_known_filenames')
    def test_when_file_last_modified_changed_returns_true(
        self, mock_generate_known_filenames, mock_os
    ):
        # mock os.stat so that file timestamps change for each call
        mock_os.stat.side_effect = [
            Mock(st_mtime=9999.8888),
            Mock(st_mtime=9999.9999),
        ]
        mock_generate_known_filenames.return_value = ['file1.py']
        assert autoreload._code_has_changed() is False
        assert len(autoreload.FILES_LAST_MODIFIED_VALUES) > 0

        assert autoreload._code_has_changed() is True


class TestMonitorNeedsReloading:

    @patch('nameko.utils.autoreload._ensure_echo_on')
    @patch('nameko.utils.autoreload._code_has_changed')
    def test_exits_with_reload_exit_code_when_code_changed(
        self, mock_code_has_changed, mock_ensure_echo_on
    ):
        mock_code_has_changed.side_effect = [False, True]
        with pytest.raises(SystemExit) as exc:
            autoreload._monitor_needs_reloading()
        assert exc.value.code == autoreload.RELOAD_EXIT_CODE


class TestRestartApplication:
    @patch('nameko.utils.autoreload.os')
    @patch('nameko.utils.autoreload.sys')
    def test_exits_with_process_code_if_exit_code_not_reload_exit_code(
        self, mock_sys, mock_os
    ):
        mock_os.spawnve.return_value = 1
        assert autoreload._restart_application_with_autoreload() == 1


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
