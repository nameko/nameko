"""
Provides classes and method to deal with dependency injection.
"""
from abc import ABCMeta, abstractmethod
from functools import wraps
import inspect
from itertools import chain
import types


from nameko.utils import SpawningSet


ENTRYPOINT_PROVIDERS_ATTR = 'nameko_providers'


class NotInitializedError(Exception):
    pass


class DependencyProvider(object):

    name = None

    def prepare(self, srv_ctx):
        """ Called when the service container starts.

        DependencyProviders should do any required initialisation here.

        Args:
            - srv_ctx: see ``nameko.service.ServiceContainer.ctx``
        """

    def start(self, srv_ctx):
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

    def call_setup(self, worker_ctx):
        """ Called before a service worker executes a task.

        DependencyProviders should do any pre-processing here, raising
        exceptions in the event of failure.

        Example: ...

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

    def kill(self, srv_ctx, exc=None):
        """ Called to stop this dependency without grace. The exception
        causing the kill may be provided.

        DependencyProviders should urgently shut down here. This method must
        return within ``nameko.service.KILL_TIMEOUT`` seconds, otherwise it
        may be forcibly stopped.
        """


class EntrypointProvider(DependencyProvider):
    pass


class InjectionProvider(DependencyProvider):
    __metaclass__ = ABCMeta

    @abstractmethod
    def acquire_injection(self, worker_ctx):
        """ A subclass must return the instance that should be injected
        into the worker instance of the service by the container.
        """

    def release_injection(self, worker_ctx):
        """ A subclass may use this method to handle any cleanup of
        the injection.

        By default the injection will be deleted from the worker instance
        during the call_teardown.
        """

    def call_setup(self, worker_ctx):
        injection = self.acquire_injection(worker_ctx)

        injection_name = self.name
        service = worker_ctx.service
        setattr(service, injection_name, injection)

    def call_result(self, worker_ctx, result=None, exc=None):
        """ Called with the result of a service worker execution.

        DependencyProviders that need to process the result should do it here.
        Note that all DependencyProviders defining this method will be called,
        not just those that initiated the worker.

        Example: a database session provider may commit the transaction

        Args:
            - worker_ctx: see ``nameko.service.ServiceContainer.spawn_worker``
        """

    def call_teardown(self, worker_ctx):
        self.release_injection(worker_ctx)

        service = worker_ctx.service
        injection_name = self.name
        delattr(service, injection_name)


class DependencySet(SpawningSet):

    @property
    def injections(self):
        """ A ``SpawningSet`` of just the ``InjectionProvider`` instances in
        this set.
        """
        return SpawningSet([item for item in self
                            if is_injection_provider(item)])

    @property
    def entrypoints(self):
        """ A ``SpawningSet`` of just the ``EntrypointProvider`` instances in
        this set.
        """
        return SpawningSet([item for item in self
                            if is_entrypoint_provider(item)])


def register_provider(fn, provider):
    providers = getattr(fn, ENTRYPOINT_PROVIDERS_ATTR, None)

    if providers is None:
        providers = set()
        setattr(fn, ENTRYPOINT_PROVIDERS_ATTR, providers)

    providers.add(provider)


def entrypoint(decorator_func):
    """ Transform a function into a decorator that can be used to declare
    entrypoints.

    e.g::

        @entrypoint
        def http(bind_port=80):
            return HttpEntrypoint(bind_port)

        class Service(object):

            @http
            def foobar():
                pass

    """
    @wraps(decorator_func)
    def wrapper(*args, **kwargs):
        def registering_decorator(fn):
            provider = decorator_func(*args, **kwargs)
            register_provider(fn, provider)
            return fn

        # if the decorator does not use args itself,
        # i.e. it does not return a dacorator
        if len(args) == 1 and isinstance(args[0], types.FunctionType):
            fn = args[0]
            register_provider(fn, decorator_func())
            return fn
        else:
            return registering_decorator

    return wrapper


def is_injection_provider(obj):
    return isinstance(obj, InjectionProvider)


def is_entrypoint_provider(obj):
    return isinstance(obj, EntrypointProvider)


def get_injection_providers(obj):
    return inspect.getmembers(obj, is_injection_provider)


def get_entrypoint_providers(obj):
    for name, attr in inspect.getmembers(obj, inspect.ismethod):
        providers = getattr(attr, ENTRYPOINT_PROVIDERS_ATTR, [])
        for provider in providers:
            yield name, provider


def get_dependencies(obj):
    return chain(get_injection_providers(obj), get_entrypoint_providers(obj))
