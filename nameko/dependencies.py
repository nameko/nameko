'''
Provides classes and method to deal with dependency injection.

Note:
The API of this module is very unstable and serves only
as a proof of concept.

see: https://onefinestay.atlassian.net/browse/OFS-397
'''
import inspect


class DependencyProvider(object):
    pass


def is_dependency_provider(obj):
    '''
    Returns true if the obj is a DependencyProvider.

    This helper function can be used as a predicate for inspect.getmembers()
    '''
    return isinstance(obj, DependencyProvider)


def inject_dependencies(service, container):
    for name, provider in inspect.getmembers(service, is_dependency_provider):
        setattr(service, name, provider.get_instance(container))
