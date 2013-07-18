"""
Provides classes and method to deal with dependency injection.
"""
from functools import wraps
import inspect
import weakref

import eventlet
from kombu.mixins import ConsumerMixin
from kombu import BrokerConnection


queue_consumers = weakref.WeakKeyDictionary()


class QueueConsumer(ConsumerMixin):
    def __init__(self, container):
        self.container = container

        self._registry = []
        self.connection = BrokerConnection(self.container.config['amqp_uri'])

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[queue], callbacks=[callback])
                for queue, callback in self._registry]

    def register(self, queue, callback):
        self._registry.append((queue, callback))

    def start(self):
        """ reentrant, start consuming
        """
        eventlet.spawn(self.run)
        # self.run still isn't ready unless we sleep for a bit
        eventlet.sleep(1)


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

    def start(self):
        """ Called when the service container starts.

        DependencyProviders should do any required initialisation here.
        """

    def on_container_started(self):
        """ Called when the service container has successfully started.

        This is only called after all other DependencyProviders have
        successfully initialised. If the DependencyProvider listens to
        external events, they may now start acting upon them.
        """

    def stop(self):
        """ Called when the service container begins to shut down.

        DependencyProviders should do any graceful shutdown here.
        """

    def on_container_stopped(self):
        """ Called when the service container stops.

        If the DependencyProvider has not gracefully shut down, probably
        raise an error here.
        """

    def call_setup(self, method, args, kwargs):
        """ Called before a service worker executes a task.

        DependencyProviders should do any pre-processing here, raising
        exceptions in the event of failure.

        Example: ...
        """

    def call_result(self, method, result):
        """ Called with the result of a service worker execution.

        DependencyProviders that need to process the result should do it here.

        Example: a database session provider may commit the transaction
        """

    def call_teardown(self, method, result):
        """ Called after a service worker has executed a task.

        DependencyProviders should do any post-processing here, raising
        exceptions in the event of failure.

        Example: a database session provider may close the session
        """


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
