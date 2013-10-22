"""
Provides classes and method to deal with dependency injection.
"""
from abc import ABCMeta, abstractmethod
from functools import wraps, partial
import inspect
from itertools import chain
import types


from nameko.utils import SpawningSet


ENTRYPOINT_PROVIDERS_ATTR = 'nameko_providers'


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


def register_descriptor(fn, provider):
    descriptors = getattr(fn, ENTRYPOINT_PROVIDERS_ATTR, None)

    if descriptors is None:
        descriptors = set()
        setattr(fn, ENTRYPOINT_PROVIDERS_ATTR, descriptors)

    descriptors.add(provider)


class DependencyDescriptor(object):
    def __init__(self, dep_cls, init_args=None, init_kwargs=None):
        self.dep_cls = dep_cls
        self.args = init_args


def entrypoint(decorator_func):
    """ Transform a function into a decorator that can be used to declare
    entrypoints.

    e.g::

        @entrypoint
        def http(bind_port=80):
            return HttpEntrypoint(bind_port)

        class Service(object):

            @http
            def foobar()
                pass

    """
    def registering_decorator(fn, args, kwargs):
        tpe_init_args = decorator_func(*args, **kwargs)
        descriptor = DependencyDescriptor(tpe_init_args[0], tpe_init_args[1:])
        register_descriptor(fn, descriptor)
        return fn

    @wraps(decorator_func)
    def wrapper(*args, **kwargs):
        if len(args) == 1 and isinstance(args[0], types.FunctionType):
            # entrypoint_decorator is used like
            # @foobar
            # def spam():
            #     pass
            return registering_decorator(args[0], tuple(), {})
        else:
            # entrypoint_decorator is used like
            # @foobar('shrub', ...)
            # def spam():
            #     pass
            return partial(registering_decorator, args=args, kwargs=kwargs)

    return wrapper


def injection(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        tpe_init_args = fn(*args, **kwargs)
        return DependencyDescriptor(tpe_init_args[0], tpe_init_args[1:])
    return wrapped


def is_dependency_descriptor(obj):
    return isinstance(obj, DependencyDescriptor)


def is_injection_provider(obj):
    return isinstance(obj, InjectionProvider)


def is_entrypoint_provider(obj):
    return isinstance(obj, EntrypointProvider)


def get_injection_providers(obj):
    for name, descr in inspect.getmembers(obj, is_dependency_descriptor):
        yield name, descr.dep_cls(*descr.args)


def get_entrypoint_providers(obj):
    for name, attr in inspect.getmembers(obj, inspect.ismethod):
        descriptors = getattr(attr, ENTRYPOINT_PROVIDERS_ATTR, [])
        for descr in descriptors:
            yield name, descr.dep_cls(*descr.args)


def get_dependencies(obj):
    return chain(get_injection_providers(obj), get_entrypoint_providers(obj))
