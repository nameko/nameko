"""
Provides classes and method to deal with dependency injection.
"""
from __future__ import absolute_import

from abc import ABCMeta, abstractmethod
from functools import wraps, partial
import inspect
from itertools import chain
import types
from weakref import WeakSet, WeakKeyDictionary

from eventlet.event import Event
from nameko.utils import SpawningSet

from logging import getLogger
_log = getLogger(__name__)

ENTRYPOINT_PROVIDERS_ATTR = 'nameko_entrypoints'


# constants for dependency sharing
CONTAINER_SHARED = object()
# PROCESS_SHARED also serves as weakref-able sentinel obj
PROCESS_SHARED = type('process', (), {})()


class NotInitializedError(Exception):
    pass


class DependencyTypeError(TypeError):
    pass


class DependencyProvider(object):

    def prepare(self):
        """ Called when the service container starts.

        DependencyProviders should do any required initialisation here.
        """

    def start(self):
        """ Called when the service container has successfully started.

        This is only called after all other DependencyProviders have
        successfully initialised. If the DependencyProvider listens to
        external events, they may now start acting upon them.
        """

    def stop(self):
        """ Called when the service container begins to shut down.

        DependencyProviders should do any graceful shutdown here.
        """

    def kill(self):
        """ Called to stop this dependency without grace.

        DependencyProviders should urgently shut down here. This means
        stopping as soon as possible with ommiting important cleanup.
        This may be distinct from ``stop()`` for certain dependencies.

        For example, :class:`~messaging.QueueConsumer` tracks messages being
        processed and pending message acks. Its ``kill`` implementation
        discards these and disconnects from rabbit as soon as possible.
        """

    def worker_setup(self, worker_ctx):
        """ Called before a service worker executes a task. This method is
        called for all DependencyProviders, not just the one that triggered
        the worker spawn.

        DependencyProviders should do any pre-processing here, raising
        exceptions in the event of failure.

        Example: ...

        Args:
            - worker_ctx: see
                ``nameko.containers.ServiceContainer.spawn_worker``
        """

    def worker_teardown(self, worker_ctx):
        """ Called after a service worker has executed a task. This method is
        called for all DependencyProviders, not just the one that triggered
        the worker spawn.

        DependencyProviders should do any post-processing here, raising
        exceptions in the event of failure.

        Example: a database session provider may commit the session

        Args:
            - worker_ctx: see
                ``nameko.containers.ServiceContainer.spawn_worker``
        """

    def bind(self, name, container):
        """ Bind this DependencyProvider instance to ``container`` using the
        given ``name`` to identify the resource on the hosted service.

        Called during ServiceContainer initialisation. The DependencyProvider
        instance is created and then bound to the ServiceContainer instance
        controlling its lifecyle.
        """
        self.name = name
        self.container = container

    @property
    def nested_dependencies(self):
        """ Recusively yield nested dependencies of DependencyProvider.

        For example, with an instance of `:class:~nameko.rpc.RpcProvider`
        called ``rpc``::

            >>> deps = list(rpc.nested_dependencies)
            >>> deps
            [<nameko.rpc.RpcConsumer object at 0x10d125690>,
             <nameko.messaging.QueueConsumer object at 0x10d5b8f50>]
            >>>
        """
        for _, attr in inspect.getmembers(self):
            if isinstance(attr, DependencyProvider):
                yield attr
                for nested_dep in attr.nested_dependencies:
                    yield nested_dep

    def __str__(self):
        try:
            return '<{} [{}.{}] at 0x{:x}>'.format(
                type(self).__name__,
                self.container.service_name, self.name,
                id(self))
        except:
            return '<{} [unbound] at 0x{:x}>'.format(
                type(self).__name__, id(self))


class EntrypointProvider(DependencyProvider):
    pass


class InjectionProvider(DependencyProvider):
    __metaclass__ = ABCMeta

    @abstractmethod
    def acquire_injection(self, worker_ctx):
        """ A subclass must return the instance that should be injected
        into the worker instance of the service by the container.
        """

    def worker_result(self, worker_ctx, result=None, exc_info=None):
        """ Called with the result of a service worker execution.

        InjectionProvider that need to process the result should do it here.
        This method is called for all InjectionProviders on completion of any
        worker.

        Example: a database session provider may flush the transaction

        Args:
            - worker_ctx: see
                ``nameko.containers.ServiceContainer.spawn_worker``
        """

    def inject(self, worker_ctx):
        injection = self.acquire_injection(worker_ctx)

        injection_name = self.name
        service = worker_ctx.service
        setattr(service, injection_name, injection)

    def release(self, worker_ctx):

        service = worker_ctx.service
        injection_name = self.name
        delattr(service, injection_name)


class ProviderCollector(object):
    def __init__(self, *args, **kwargs):
        self._providers = set()
        self._providers_registered = False
        self._last_provider_unregistered = Event()
        super(ProviderCollector, self).__init__(*args, **kwargs)

    def register_provider(self, provider):
        self._providers_registered = True
        _log.debug('registering provider %s for %s', provider, self)
        self._providers.add(provider)

    def unregister_provider(self, provider):
        providers = self._providers
        if provider not in self._providers:
            return

        _log.debug('unregistering provider %s for %s', provider, self)

        providers.remove(provider)
        if len(providers) == 0:
            _log.debug('last provider unregistered for %s', self)
            self._last_provider_unregistered.send()

    def wait_for_providers(self):
        """ Wait for any providers registered with the collector to have
        unregistered.

        Returns immediately if no providers were ever registered.
        """
        if self._providers_registered:
            _log.debug('waiting for providers to unregister %s', self)
            self._last_provider_unregistered.wait()
            _log.debug('all providers unregistered %s', self)

    def stop(self):
        """ Default `:meth:DependencyProvider.stop()` implementation for
        subclasses using `ProviderCollector` as a mixin.
        """
        self.wait_for_providers()


class DependencySet(SpawningSet):

    @property
    def injections(self):
        """ A ``SpawningSet`` of just the ``InjectionProvider`` instances in
        this set.
        """
        return SpawningSet(item for item in self
                           if is_injection_provider(item))

    @property
    def entrypoints(self):
        """ A ``SpawningSet`` of just the ``EntrypointProvider`` instances in
        this set.
        """
        return SpawningSet(item for item in self
                           if is_entrypoint_provider(item))

    @property
    def nested(self):
        """ A ``SpawningSet`` of any nested dependency instances in this set.
        """
        all_deps = self
        return all_deps - self.injections - self.entrypoints


registered_dependencies = WeakSet()
registered_injections = WeakSet()


def register_dependency(factory):
    registered_dependencies.add(factory)


def register_injection(factory):
    registered_injections.add(factory)


def register_entrypoint(fn, provider):
    descriptors = getattr(fn, ENTRYPOINT_PROVIDERS_ATTR, None)

    if descriptors is None:
        descriptors = set()
        setattr(fn, ENTRYPOINT_PROVIDERS_ATTR, descriptors)

    descriptors.add(provider)


shared_dependencies = WeakKeyDictionary()


class DependencyFactory(object):

    sharing_key = None

    def __init__(self, dep_cls, *init_args, **init_kwargs):
        self.dep_cls = dep_cls
        self.args = init_args
        self.kwargs = init_kwargs

        # keep a reference to every created instance
        self.instances = WeakSet()

    @property
    def key(self):
        return (self.dep_cls, str(self.args), str(self.kwargs))

    def create_and_bind_instance(self, name, container):
        """ Instantiate ``dep_cls`` and bind it to ``container``.

        See `:meth:~DependencyProvider.bind`.
        """
        sharing_key = self.sharing_key
        if sharing_key is not None:
            if sharing_key is CONTAINER_SHARED:
                sharing_key = container

            shared_dependencies.setdefault(sharing_key, {})
            instance = shared_dependencies[sharing_key].get(self.key)
            if instance is None:
                instance = self.dep_cls(*self.args, **self.kwargs)
                shared_dependencies[sharing_key][self.key] = instance
        else:
            instance = self.dep_cls(*self.args, **self.kwargs)
        instance.bind(name, container)

        for name, attr in inspect.getmembers(instance):
            if isinstance(attr, DependencyFactory):
                prov = attr.create_and_bind_instance(name, container)
                setattr(instance, name, prov)

        self.instances.add(instance)
        return instance


def entrypoint(decorator_func):
    """ Transform a callable into a decorator that can be used to declare
    entrypoints.

    The callable must return a DependencyFactory that creates the
    EntrypointProvider instance.

    The returned ``wrapper`` function has a ``provider_cls`` attribute that
    references the EntrypointProvider subclass returned by its factory. This
    helps separate the ``@entrypoint`` decorated methods from the class
    that implements the dependency - users only need to refer to the method
    and nameko can determine the implementing class.

    e.g::

        @entrypoint
        def http(bind_port=80):
            return DependencyFactory(HttpEntrypoint, bind_port)

        class Service(object):

            @http
            def foobar():
                pass

    """
    def registering_decorator(wrapper, fn, args, kwargs):
        """ Verify that ``decorator_func`` returns a DependencyFactory and
        register ``fn`` as an entrypoint.

        Saves a reference to the provider class of the factory onto
        the ``wrapper`` function that wraps ``decorator_func``.
        """
        factory = decorator_func(*args, **kwargs)
        if not isinstance(factory, DependencyFactory):
            raise DependencyTypeError('Arguments to `entrypoint` must return '
                                      'DependencyFactory instances')
        register_entrypoint(fn, factory)
        wrapper.provider_cls = factory.dep_cls

        return fn

    @wraps(decorator_func)
    def wrapper(*args, **kwargs):
        if len(args) == 1 and isinstance(args[0], types.FunctionType):
            # usage without arguments to the decorator:
            # @foobar
            # def spam():
            #     pass
            return registering_decorator(wrapper, args[0], tuple(), {})
        else:
            # usage with arguments to the decorator:
            # @foobar('shrub', ...)
            # def spam():
            #     pass
            return partial(registering_decorator,
                           wrapper, args=args, kwargs=kwargs)

    return wrapper


def injection(fn):
    """ Transform a callable into a function that can be used to create
    injections.

    The callable must return a DependencyFactory that creates the
    InjectionProvider instance.

    The returned ``wrapped`` function has a ``provider_cls`` attribute that
    references the InjectionProvider subclass returned by the factory. This
    helps separate the ``@injection`` decorated methods from the class
    that implements the dependency - users only need to refer to the method
    and nameko can determine the implementing class.

    e.g::

        @injection
        def database(*args, **kwargs):
            return DependencyFactory(DatabaseProvider, *args, **kwargs)

        class Service(object):

            db = database()
    """
    @wraps(fn)
    def wrapped(*args, **kwargs):
        factory = fn(*args, **kwargs)
        if not isinstance(factory, DependencyFactory):
            raise DependencyTypeError('Arguments to `injection` must return '
                                      'DependencyFactory instances')
        register_injection(factory)
        wrapped.provider_cls = factory.dep_cls
        return factory
    return wrapped


def dependency(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        sharing_key = kwargs.pop('shared', None)

        factory = fn(*args, **kwargs)
        if not isinstance(factory, DependencyFactory):
            raise DependencyTypeError('Arguments to `dependency` must return '
                                      'DependencyFactory instances')
        factory.sharing_key = sharing_key
        register_dependency(factory)
        return factory
    return wrapped


def is_injection_provider(obj):
    return isinstance(obj, InjectionProvider)


def is_entrypoint_provider(obj):
    return isinstance(obj, EntrypointProvider)


def prepare_injection_providers(container, include_dependencies=False):
    service_cls = container.service_cls
    for name, attr in inspect.getmembers(service_cls):
        if attr in registered_injections:
            factory = attr
            provider = factory.create_and_bind_instance(name, container)
            yield provider
            if include_dependencies:
                for dependency in provider.nested_dependencies:
                    yield dependency


def prepare_entrypoint_providers(container, include_dependencies=False):
    service_cls = container.service_cls
    for name, attr in inspect.getmembers(service_cls, inspect.ismethod):
        factories = getattr(attr, ENTRYPOINT_PROVIDERS_ATTR, [])
        for factory in factories:
            provider = factory.create_and_bind_instance(name, container)
            yield provider
            if include_dependencies:
                for dependency in provider.nested_dependencies:
                    yield dependency


def prepare_dependencies(container):
    return chain(
        prepare_injection_providers(container, include_dependencies=True),
        prepare_entrypoint_providers(container, include_dependencies=True))
