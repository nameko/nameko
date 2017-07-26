from functools import wraps
import os
import signal
import sys
import time
import termios
import traceback

from six.moves import _thread as thread

RELOAD_EXIT_CODE = 8
FILES_LAST_MODIFIED_VALUES = {}
APP_EXCEPTION = None
ERROR_FILES = []
CACHED_MODULES = set()
CACHED_FILENAMES = []


def make_autoreload(app_run_func, args=(), kwargs=None):
    wrapped_app_run_func = _raise_app_errors(app_run_func)
    try:
        _reloader(wrapped_app_run_func, args, kwargs or {})
    except KeyboardInterrupt:  # pragma: no cover
        pass


def _generate_known_filenames(only_include_new_files=False):
    global CACHED_MODULES, CACHED_FILENAMES
    module_values = set(sys.modules.values())
    CACHED_FILENAMES = _clean_python_files(CACHED_FILENAMES)
    if CACHED_MODULES == module_values:
        # no changes
        if only_include_new_files:
            return []
        else:
            return CACHED_FILENAMES + _clean_python_files(ERROR_FILES)

    new_modules = module_values - CACHED_MODULES
    new_filenames = _clean_python_files(
        filename.__file__ for filename in new_modules
        if hasattr(filename, '__file__')
    )
    # update the cache
    CACHED_MODULES = CACHED_MODULES | new_modules
    CACHED_FILENAMES += new_filenames
    if only_include_new_files:
        return new_filenames + _clean_python_files(ERROR_FILES)
    else:
        return CACHED_FILENAMES + _clean_python_files(ERROR_FILES)


def _code_has_changed():
    global FILES_LAST_MODIFIED_VALUES
    for filename in _generate_known_filenames():
        file_last_modified = os.stat(filename).st_mtime
        if filename not in FILES_LAST_MODIFIED_VALUES:
            FILES_LAST_MODIFIED_VALUES[filename] = file_last_modified
            continue
        if file_last_modified != FILES_LAST_MODIFIED_VALUES[filename]:
            FILES_LAST_MODIFIED_VALUES = {}
            try:
                del ERROR_FILES[ERROR_FILES.index(filename)]
            except ValueError:
                pass
            return True
    return False


def _clean_python_files(file_paths):
    clean_file_paths = []
    for f_path in file_paths:
        if not f_path:
            continue  # pragma: no cover
        if f_path.endswith('.pyc') or f_path.endswith('.pyo'):
            f_path = f_path[:-1]
        if f_path.endswith('$py.class'):
            f_path = '{}.py'.format(f_path[:-len('$py.class')])
        if f_path and os.path.exists(f_path):
            clean_file_paths.append(f_path)
    return clean_file_paths


def _ensure_echo_on():  # pragma: no cover
    if not sys.stdin.isatty():
        return None
    file_descriptor_attrs = termios.tcgetattr(sys.stdin)
    _, _, _, lflag, _, _, _ = file_descriptor_attrs
    if not lflag & termios.ECHO:
        lflag |= termios.ECHO
        try:
            old_sig_handler = signal.signal(signal.SIGTTOU, signal.SIG_IGN)
        except AttributeError:
            old_sig_handler = None
        termios.tcsetattr(sys.stdin, termios.TCSANOW, file_descriptor_attrs)
        if old_sig_handler is not None:
            signal.signal(signal.SIGTTOU, old_sig_handler)


def _monitor_needs_reloading():
    _ensure_echo_on()
    while True:
        if _code_has_changed():
            # force a reload
            sys.exit(RELOAD_EXIT_CODE)
        time.sleep(1)


def _restart_application_with_autoreload():
    while True:
        new_environ = os.environ.copy()
        new_environ['RUN_MAIN_APP'] = 'true'
        exit_code = os.spawnve(
            os.P_WAIT,
            sys.executable,
            [sys.executable] + sys.argv,
            new_environ
        )
        if exit_code != RELOAD_EXIT_CODE:
            return exit_code


def _reloader(app_run_func, args, kwargs):
    if os.environ.get('RUN_MAIN_APP') != 'true':
        exit_code = _restart_application_with_autoreload()
        if exit_code < 0:
            os.kill(os.getpid(), -exit_code)
        else:
            sys.exit(exit_code)
    else:
        thread.start_new_thread(app_run_func, args, kwargs)
        _monitor_needs_reloading()


def _raise_app_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        global APP_EXCEPTION
        try:
            func(*args, **kwargs)
        except Exception:
            APP_EXCEPTION = sys.exc_info()
            _, exc_value, exc_traceback = APP_EXCEPTION
            try:
                filename = exc_value.filename
            except AttributeError:
                # fallback, get filename from the last item in the stack
                filename = traceback.extract_tb(exc_traceback)[-1][0]
            if filename not in ERROR_FILES:
                ERROR_FILES.append(filename)
            raise
    return wrapper
