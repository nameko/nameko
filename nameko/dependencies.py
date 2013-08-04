"""
Provides classes and method to deal with dependency injection.
"""
from functools import wraps
import inspect
from itertools import chain
import types


from nameko.utils import SpawningSet


DECORATOR_PROVIDERS_ATTR = 'nameko_providers'


class NotInitializedError(Exception):
    pass


class DependencyProvider(object):

    name = None

    def start(self, srv_ctx):
        """ Called when the service container starts.

        DependencyProviders should do any required initialisation here.

        Args:
            - srv_ctx: see ``nameko.service.ServiceContainer.ctx``
        """

    def on_container_started(self, srv_ctx):
        """ Called when the service container has successfully started.

        This is only called after all other DependencyProviders have
        successfully initialised. If the DependencyProvider listens to
        external events, they may now start acting upon them.

        Args:
            - srv_ctx: see ``nameko.service.ServiceContainer.ctx``
        """

    def stop(self, srv_ctx):
        """ Called when the service container begins to shut down.

        DependencyProviders should do any graceful shutdown here.

        Args:
            - srv_ctx: see ``nameko.service.ServiceContainer.ctx``
        """

    def on_container_stopped(self, srv_ctx):
        """ Called when the service container stops.

        If the DependencyProvider has not gracefully shut down, probably
        raise an error here.

        Args:
            - srv_ctx: see ``nameko.service.ServiceContainer.ctx``
        """

    def call_setup(self, worker_ctx):
        """ Called before a service worker executes a task.

        DependencyProviders should do any pre-processing here, raising
        exceptions in the event of failure.

        Example: ...

        Args:
            - worker_ctx: see ``nameko.service.ServiceContainer.spawn_worker``
        """

    def call_result(self, worker_ctx, result=None, exc=None):
        """ Called with the result of a service worker execution.

        DependencyProviders that need to process the result should do it here.

        Example: a database session provider may commit the transaction

        Args:
            - worker_ctx: see ``nameko.service.ServiceContainer.spawn_worker``
        """

    def call_teardown(self, worker_ctx):
        """ Called after a service worker has executed a task.

        DependencyProviders should do any post-processing here, raising
        exceptions in the event of failure.

        Example: a database session provider may close the session

        Args:
            - worker_ctx: see ``nameko.service.ServiceContainer.spawn_worker``
        """


class DecoratorDependency(DependencyProvider):
    pass


class AttributeDependency(DependencyProvider):
    pass


class DependencySet(SpawningSet):

    @property
    def attributes(self):
        """ A ``SpawningSet`` of just the ``AttributeDependency`` instances in
        this set.
        """
        return SpawningSet([item for item in self
                            if is_attribute_dependency(item)])

    @property
    def decorators(self):
        """ A ``SpawningSet`` of just the ``DecoratorDependency`` instances in
        this set.
        """
        return SpawningSet([item for item in self
                            if is_decorator_dependency(item)])


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

        # if the providor_docorator does not use args itself,
        # i.e. it does not return a dacorator
        if len(args) == 1 and isinstance(args[0], types.FunctionType):
            fn = args[0]
            register_provider(fn, provider_decorator())
            return fn
        else:
            return registering_decorator

    return wrapper


def is_dependency_provider(obj):
    """
    Returns true if the obj is a DependencyProvider.

    This helper function can be used as a predicate for inspect.getmembers()
    """
    return isinstance(obj, DependencyProvider)


def is_attribute_dependency(obj):
    return isinstance(obj, AttributeDependency)


def is_decorator_dependency(obj):
    return isinstance(obj, DecoratorDependency)


def get_attribute_providers(obj):
    return inspect.getmembers(obj, is_attribute_dependency)


def get_decorator_providers(obj):
    for name, attr in inspect.getmembers(obj, inspect.ismethod):
        providers = getattr(attr, DECORATOR_PROVIDERS_ATTR, [])
        for provider in providers:
            yield name, provider


def get_dependencies(obj):
    return chain(get_attribute_providers(obj), get_decorator_providers(obj))


def get_providers(fn, filter_type=object):
    providers = getattr(fn, DECORATOR_PROVIDERS_ATTR, [])
    for provider in providers:
        if isinstance(provider, filter_type):
            yield provider


def inject_dependencies(service, container):
    for name, provider in inspect.getmembers(service, is_dependency_provider):
        setattr(service, name, provider.get_instance(container))
