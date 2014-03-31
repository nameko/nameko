"""
Utilities for testing DependencyProviders
"""

from contextlib import contextmanager

from mock import patch, Mock


@contextmanager
def patch_injection_provider(provider):
    """ Patches an `InjectionProvider` provider's acquire_injection
    such that it returns a `Mock` as the injection object.
    The injection object will be yielded by the contextmanager.

    .. admonition:: Deprecated

        ``patch_injection_provider`` is deprecated in favour of
        :meth:`nameko.testing.services.worker_factory`
    """
    injection = Mock()

    with patch.object(provider, 'acquire_injection', autospec=True) as acquire:
        acquire.return_value = injection
        yield injection
