"""
Utilities for testing nameko services.
"""

from collections import OrderedDict
from contextlib import contextmanager
import inspect

from eventlet import event
from mock import MagicMock

from nameko.dependencies import (
    DependencyFactory, InjectionProvider, EntrypointProvider, entrypoint)
from nameko.exceptions import DependencyNotFound


@contextmanager
def entrypoint_hook(container, name, context_data=None):
    """ Yield a function providing an entrypoint into a hosted service.

    The yielded function may be called as if it were the bare method defined
    in the service class. Intended to be used as an integration testing
    utility.

    **Usage**

    To verify that ServiceX and ServiceY are compatible, make an integration
    test that checks their interaction:

    .. literalinclude:: examples/testing/integration_test.py

    """
    provider = next((entrypoint for entrypoint in container.entrypoints
                     if entrypoint.name == name), None)

    if provider is None:
        raise DependencyNotFound("No entrypoint called '{}' found "
                                 "on container {}.".format(name, container))

    def hook(*args, **kwargs):
        result = event.Event()

        def handle_result(worker_ctx, res=None, exc_info=None):
            result.send(res, exc_info)
            return res, exc_info

        container.spawn_worker(provider, args, kwargs,
                               context_data=context_data,
                               handle_result=handle_result)
        return result.wait()

    yield hook


def worker_factory(service_cls, **injections):
    """ Return an instance of ``service_cls`` with its injected dependencies
    replaced with Mock objects, or as given in ``injections``.

    **Usage**

    The following example service proxies calls to a "math" service via
    and ``rpc_proxy`` injection::

        from nameko.rpc import rpc_proxy, rpc

        class ConversionService(object):
            math = rpc_proxy("math_service")

            @rpc
            def inches_to_cm(self, inches):
                return self.math.multiply(inches, 2.54)

            @rpc
            def cm_to_inches(self, cms):
                return self.math.divide(cms, 2.54)

    Use the ``worker_factory`` to create an unhosted instance of
    ``ConversionService`` with its injections replaced by Mock objects::

        service = worker_factory(ConversionService)

    Nameko's entrypoints do not modify the service methods, so they can be
    called directly on an unhosted instance. The injection Mocks can be used
    as any other Mock object, so a complete unit test for Service may look
    like this::

        # create worker instance
        service = worker_factory(Service)

        # replace "math" service
        service.math.multiply.side_effect = lambda x, y: x * y
        service.math.divide.side_effect = lambda x, y: x / y

        # test inches_to_cm business logic
        assert service.inches_to_cm(300) == 762
        service.math.multiply.assert_called_once_with(300, 2.54)

        # test cms_to_inches business logic
        assert service.cms_to_inches(762) == 300
        service.math.divide.assert_called_once_with(762, 2.54)

    *Providing Injections*

    The ``**injections`` kwargs to ``worker_factory`` can be used to provide
    a replacement injection instead of a Mock. For example, to unit test a
    service against a real database:

    .. literalinclude:: examples/testing/unit_with_provided_injection_test.py

    """
    service = service_cls()
    for name, attr in inspect.getmembers(service):
        if isinstance(attr, DependencyFactory):
            factory = attr
            if issubclass(factory.dep_cls, InjectionProvider):
                try:
                    injection = injections[name]
                except KeyError:
                    injection = MagicMock()
                setattr(service, name, injection)
    return service


class MockInjection(InjectionProvider):
    def __init__(self, name):
        self.name = name
        self.injection = MagicMock()

    def acquire_injection(self, worker_ctx):
        return self.injection


def replace_injections(container, *injections):
    """ Replace the injections on ``container`` with :class:`MockInjection`
    objects if they are named in ``injections``.

    Return the :attr:`MockInjection.injection` of the replacements, so that
    calls to the replaced injections can be inspected. Return a single object
    if only one injection was replaced, and a generator yielding the
    replacements in the same order as ``names`` otherwise.

    Replacements are made on the container instance and have no effect on the
    service class. New container instances are therefore unaffected by
    replacements on previous instances.

    **Usage**

    ::

        from nameko.rpc import rpc_proxy, rpc
        from nameko.standalone.rpc import RpcProxy

        class ConversionService(object):
            math = rpc_proxy("math_service")

            @rpc
            def inches_to_cm(self, inches):
                return self.math.multiply(inches, 2.54)

            @rpc
            def cm_to_inches(self, cms):
                return self.math.divide(cms, 2.54)

        container = ServiceContainer(ConversionService, config)
        math = replace_injections(container, "math")

        container.start()

        with RpcProxy('conversionservice', config) as proxy:
            proxy.cm_to_inches(100)

        # assert that the injection was called as expected
        math.divide.assert_called_once_with(100, 2.54)

    """
    if container.started:
        raise RuntimeError('You must replace injections before the '
                           'container is started.')

    injection_deps = list(container.injections)
    injection_names = {dep.name for dep in injection_deps}

    missing = set(injections) - injection_names
    if missing:
        raise DependencyNotFound("Injections(s) '{}' not found on {}.".format(
            missing, container))

    replacements = OrderedDict()

    named_injections = {dep.name: dep for dep in container.injections
                        if dep.name in injections}
    for name in injections:
        dependency = named_injections[name]
        replacement = MockInjection(name)
        replacements[dependency] = replacement
        container.dependencies.remove(dependency)
        container.dependencies.add(replacement)

    # if only one name was provided, return any replacement directly
    # otherwise return a generator
    res = (replacement.injection for replacement in replacements.values())
    if len(injections) == 1:
        return next(res)
    return res


def restrict_entrypoints(container, *entrypoints):
    """ Restrict the entrypoints on ``container`` to those named in
    ``entrypoints``.

    This method must be called before the container is started.

    **Usage**

    The following service definition has two entrypoints for "method"::

        class Service(object):

            @rpc
            def foo(self, arg):
                pass

            @rpc
            @event_handler('srcservice', 'event_one')
            def bar(self, arg)
                pass

        container = container_factory(Service, config)

    To disable the entrypoints other than on "foo"::

        restrict_entrypoints(container, "foo")

    To maintain both the rpc and the event_handler entrypoints on "bar"::

        restrict_entrypoints(container, "bar")

    Note that it is not possible to identify entrypoints individually.
    """
    if container.started:
        raise RuntimeError('You must restrict entrypoints before the '
                           'container is started.')

    entrypoint_deps = list(container.entrypoints)
    entrypoint_names = {dep.name for dep in entrypoint_deps}

    missing = set(entrypoints) - entrypoint_names
    if missing:
        raise DependencyNotFound("Entrypoint(s) '{}' not found on {}.".format(
            missing, container))

    for dependency in entrypoint_deps:
        if dependency.name not in entrypoints:
            container.dependencies.remove(dependency)


class DummyEntrypoint(EntrypointProvider):
    pass


@entrypoint
def dummy():
    return DependencyFactory(DummyEntrypoint)
