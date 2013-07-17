'''
Provides classes and method to deal with dependency injection.

Note:
The API of this module is very unstable and serves only
as a proof of concept.

see: https://onefinestay.atlassian.net/browse/OFS-397
'''

from functools import wraps
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


DECORATOR_PROVIDERS_ATTR = 'nameko_providers'


def register_provider(fn, provider):
    providers = getattr(fn, DECORATOR_PROVIDERS_ATTR, None)

    if providers is None:
        providers = set()
        setattr(fn, DECORATOR_PROVIDERS_ATTR, providers)

    providers.add(provider)


def dependency_decorator(provider_decorator):
    @wraps(provider_decorator)
    def wrapper(*args, **kwargs):
        def registering_decorator(fn):
            provider = provider_decorator(*args, **kwargs)
            register_provider(fn, provider)
            return fn

        return registering_decorator
    return wrapper


def get_decorator_providers(obj):
    for name, attr in inspect.getmembers(obj, inspect.ismethod):
        providers = get_providers(attr)
        for provider in providers:
            yield name, provider


def get_providers(fn, filter_type=object):
    providers = getattr(fn, DECORATOR_PROVIDERS_ATTR, [])
    for provider in providers:
        if isinstance(provider, filter_type):
            yield provider
