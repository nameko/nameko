"""
Provides classes and method to deal with dependency injection.
"""
import inspect


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

    def container_starting(self, container, service, name):
        """ Called when the service container starts.

        DependencyProviders should do any required initialisation here.
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


def is_dependency_provider(obj):
    """
    Returns true if the obj is a DependencyProvider.

    This helper function can be used as a predicate for inspect.getmembers()
    """
    return isinstance(obj, DependencyProvider)


def get_dependencies(service):
    return inspect.getmembers(service, is_dependency_provider)


def register_dependencies(service, container):
    dependencies = get_dependencies(service)
    for name, dependency in dependencies:
        dependency.register(name, service, container)
    return dependencies


def inject_dependencies(service, container):
    for name, provider in inspect.getmembers(service, is_dependency_provider):
        setattr(service, name, provider.get_instance(container))
