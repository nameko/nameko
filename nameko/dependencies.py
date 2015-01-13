"""
Provides classes and method to deal with dependency injection.
"""
from __future__ import absolute_import

from functools import partial
import inspect
import types
import weakref

from eventlet.event import Event

from nameko.utils import SpawningSet, repr_safe_str

from logging import getLogger
_log = getLogger(__name__)


ENTRYPOINT_EXTENSIONS_ATTR = 'nameko_entrypoints'
UNBOUND_NAME = "<unbound-extension>"

shared_extensions = weakref.WeakKeyDictionary()


class Extension(object):

    __clone = False
    __params = None

    name = UNBOUND_NAME  # property, knows if clone?
    container = None

    def __new__(cls, *args, **kwargs):
        inst = super(Extension, cls).__new__(cls, *args, **kwargs)
        inst.__params = (args, kwargs)
        return inst

    def __init__(self, *args, **kwargs):
        """ This is called both at class declaration time and at bind time,
        so avoid side-effects. You probably only want to bind paramters here.
        Do work in `{before_,}start.
        """
        super(Extension, self).__init__(*args, **kwargs)

    def before_start(self):
        """ Called before the service container starts.

        Extensions should do any required initialisation here.
        """

    def start(self):
        """ Called when the service container has successfully started.

        This is only called after all other Extensions have successfully
        returned from :meth:`Extension.before_start`. If the Extension
        listens to external events, it should now start acting upon them.
        """

    def stop(self):
        """ Called when the service container begins to shut down.

        Extensions should do any graceful shutdown here.
        """

    def kill(self):
        """ Called to stop this extension without grace.

        Extensions should urgently shut down here. This means
        stopping as soon as possible by omitting cleanup.
        This may be distinct from ``stop()`` for certain dependencies.

        For example, :class:`~messaging.QueueConsumer` tracks messages being
        processed and pending message acks. Its ``kill`` implementation
        discards these and disconnects from rabbit as soon as possible.

        Extensions should not raise during kill, since the container
        is already dying. Instead they should log what is appropriate and
        swallow the exception to allow the container kill to continue.
        """

    def bind(self, name, container):
        """ Bind a clone of this extension to `container` with the given
        `name`.
        """
        if self.__clone:  # TODO: prefer this over check in clone?
            raise RuntimeError('Cloned extensions cannot be bound.')

        # clone this extension and associate it with the container
        clone = self.clone()
        clone.name = name
        clone.container = container

        # recursively bind nested extensions
        for ext_name, ext in inspect.getmembers(self, is_extension):
            setattr(clone, ext_name, ext.bind(ext_name, container))

        return clone

    def clone(self):
        if self.__clone:  # TODO: need this even with the check in bind above?
            raise RuntimeError('Cloned extensions cannot be cloned.')

        cls = type(self)
        args, kwargs = self.__params
        instance = cls(*args, **kwargs)
        instance.__clone = True
        # alternative: del clone.clone; del clone.bind
        return instance

    @property
    def extensions(self):
        """ A generator that yields all nested extensions, including `self`
        """
        for _, ext in iter_extensions(self):
            yield ext
        yield self

    def __repr__(self):
        if self.name is UNBOUND_NAME:
            return '<{} [unbound] at 0x{:x}>'.format(  # declaration?
                type(self).__name__, id(self))

        service_name = repr_safe_str(self.container.service_name)
        name = repr_safe_str(self.name)

        return '<{} [{}.{}] at 0x{:x}>'.format(
            type(self).__name__, service_name, name, id(self))


class SharedExtension(Extension):

    @property
    def sharing_key(self):
        return type(self)

    def bind(self, name, container):
        """ Bind implementation that supports sharing.
        """
        # if there's already a cloned instance, return that
        shared_extensions.setdefault(container, {})
        shared = shared_extensions[container].get(self.sharing_key)
        if shared:
            return shared

        bound = super(SharedExtension, self).bind(name, container)

        # save the cloned instance
        shared_extensions[container][self.sharing_key] = bound

        return bound


class InjectionProvider(Extension):

    def acquire_injection(self, worker_ctx):
        """ Called before worker execution. An InjectionProvider should return
        an object to be injected into the worker instance by the container.
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

    def worker_setup(self, worker_ctx):
        """ Called before a service worker executes a task. This method is
        called for all Extensions, not just the one that triggered
        the worker spawn.

        Extensions should do any pre-processing here, raising
        exceptions in the event of failure.

        Example: ...

        Args:
            - worker_ctx: see
                ``nameko.containers.ServiceContainer.spawn_worker``
        """

    def worker_teardown(self, worker_ctx):
        """ Called after a service worker has executed a task. This method is
        called for all Extensions, not just the one that triggered
        the worker spawn.

        Extensions should do any post-processing here, raising
        exceptions in the event of failure.

        Example: a database session provider may commit the session

        Args:
            - worker_ctx: see
                ``nameko.containers.ServiceContainer.spawn_worker``
        """


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
        """ Default `:meth:Extension.stop()` implementation for
        subclasses using `ProviderCollector` as a mixin.
        """
        self.wait_for_providers()


class ExtensionSet(SpawningSet):

    @property
    def injections(self):
        """ A ``SpawningSet`` of just the ``InjectionProvider`` instances in
        this set.
        """
        return SpawningSet(item for item in self
                           if is_injection_provider(item))

    @property
    def entrypoints(self):
        """ A ``SpawningSet`` of just the ``Entrypoint`` instances in
        this set.
        """
        return SpawningSet(item for item in self if is_entrypoint(item))

    @property
    def other(self):
        """ A ``SpawningSet`` of any other dependency instances in this set.
        """
        all_deps = self
        return all_deps - self.injections - self.entrypoints


def register_entrypoint(fn, provider):
    descriptors = getattr(fn, ENTRYPOINT_EXTENSIONS_ATTR, None)

    if descriptors is None:
        descriptors = set()
        setattr(fn, ENTRYPOINT_EXTENSIONS_ATTR, descriptors)

    descriptors.add(provider)


class Entrypoint(Extension):

    @classmethod
    def entrypoint(cls, *args, **kwargs):

        def registering_decorator(fn, args, kwargs):
            instance = cls(*args, **kwargs)
            register_entrypoint(fn, instance)
            return fn

        if len(args) == 1 and isinstance(args[0], types.FunctionType):
            # usage without arguments to the decorator:
            # @foobar
            # def spam():
            #     pass
            return registering_decorator(args[0], args=(), kwargs={})
        else:
            # usage with arguments to the decorator:
            # @foobar('shrub', ...)
            # def spam():
            #     pass
            return partial(registering_decorator, args=args, kwargs=kwargs)


def is_extension(obj):
    return isinstance(obj, Extension)


def is_injection_provider(obj):
    return isinstance(obj, InjectionProvider)


def is_entrypoint(obj):
    return isinstance(obj, Entrypoint)


def iter_extensions(extension):
    for name, ext in inspect.getmembers(extension, is_extension):
        for item in iter_extensions(ext):
            yield item
        yield name, ext
