from contextlib import contextmanager

from mock import patch, Mock


@contextmanager
def patch_attr_dependency(provider):
    injection = Mock()

    with patch.object(provider, 'acquire_injection') as acquire:
        acquire.return_value = injection
        yield injection
