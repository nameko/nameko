"""
Provides classes and method to deal with dependency injection.
"""
from functools import wraps

import inspect

DECORATOR_PROVIDERS_ATTR = 'nameko_providers'


class DependencyProvider(object):

    name = None
    service = None
    container = None

    def register(self, name, service, container):
        """ Register this DependencyProvider as ``name`` on the ``service``
        within ``container``.
        """
        self.name = name
        self.service = service
        self.container = container

    def initialise(self):
        """ Called when the service container is initialised.

        DependencyProviders should do any required initialisation here.
        """

    def container_starting(self):
        """ Called when the service container starts.

        DependencyProviders should do any required startup here.
        """

    def container_started(self):
        """ Called when the service container has successfully started.

        This is only called after all other DependencyProviders have
        successfully initialised. If the DependencyProvider listens to
        external events, they may now start acting upon them.
        """

    def container_stopping(self):
        """ Called when the service container begins to shut down.

        DependencyProviders should do any graceful shutdown here.
        """

    def container_stopped(self):
        """ Called when the service container stops.

        If the DependencyProvider has not gracefully shut down, probably
        raise an error here.
        """

    def service_pre_call(self, method, args, kwargs):
        """ Called before a service worker executes a task.

        DependencyProviders should do any pre-processing here, raising
        exceptions in the event of failure.

        Example: ...
        """

    def service_post_call(self, method, result):
        """ Called after a service worker has executed a task.

        DependencyProviders should do any post-processing here, raising
        exceptions in the event of failure.

        Example: a database session provider may commit the transaction here.
        """

    def service_call_result(self):
        """ Called with the result of a service worker execution, after all
        other DependencyProviders have successfully completed their post-call
        processing.

        DependencyProviders that requested the call may want to acknowledge it
        here.

        Example: an RPC provider may provide the result to the caller here.
        """


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


def is_dependency_provider(obj):
    """
    Returns true if the obj is a DependencyProvider.

    This helper function can be used as a predicate for inspect.getmembers()
    """
    return isinstance(obj, DependencyProvider)


def get_attribute_providers(obj):
    return inspect.getmembers(obj, is_dependency_provider)


def get_decorator_providers(obj):
    for name, attr in inspect.getmembers(obj, inspect.ismethod):
        providers = getattr(attr, DECORATOR_PROVIDERS_ATTR, [])
        for provider in providers:
            yield name, provider


def get_dependencies(obj):
    return get_attribute_providers(obj) + list(get_decorator_providers(obj))


def register_dependencies(service, container):
    dependencies = get_dependencies(service)
    for name, dependency in dependencies:
        dependency.register(name, service, container)
    return [dependency for name, dependency in dependencies]


def get_providers(fn, filter_type=object):
    providers = getattr(fn, DECORATOR_PROVIDERS_ATTR, [])
    for provider in providers:
        if isinstance(provider, filter_type):
            yield provider


def inject_dependencies(service, container):
    for name, provider in inspect.getmembers(service, is_dependency_provider):
        setattr(service, name, provider.get_instance(container))

