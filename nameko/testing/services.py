"""
Utilities for testing nameko services.
"""

from collections import OrderedDict
from contextlib import contextmanager
import inspect

from eventlet import event
from mock import Mock

from nameko.dependencies import (
    get_entrypoint_providers, DependencyFactory, InjectionProvider,
    ENTRYPOINT_PROVIDERS_ATTR, is_injection_provider)
from nameko.exceptions import DependencyNotFound
from nameko.testing.utils import get_dependency


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
    provider = next((prov for prov in get_entrypoint_providers(container)
                    if prov.name == name), None)

    if provider is None:
        raise DependencyNotFound("No entrypoint called '{}' found "
                                 "on container {}.".format(name, container))

    def hook(*args, **kwargs):
        result = event.Event()

        def handle_result(worker_ctx, res=None, exc=None):
            result.send(res, exc)

        container.spawn_worker(provider, args, kwargs,
                               context_data=context_data,
                               handle_result=handle_result)
        return result.wait()

    yield hook


def instance_factory(service_cls, **injections):
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

    Use the ``instance_factory`` to create an unhosted instance of
    ``ConversionService`` with its injections replaced by Mock objects::

        service = instance_factory(ConversionService)

    Nameko's entrypoints do not modify the service methods, so they can be
    called directly on an unhosted instance. The injection Mocks can be used
    as any other Mock object, so a complete unit test for Service may look
    like this::

        # create instance
        service = instance_factory(Service)

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

    The ``**injections`` kwargs to ``instance_factory`` can be used to provide
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
                    injection = Mock()
                setattr(service, name, injection)
    return service


class MockInjection(InjectionProvider):
    def __init__(self, name):
        self.name = name
        self.injection = Mock()

    def acquire_injection(self, worker_ctx):
        return self.injection


def replace_injections(container, *names):
    """ Replace the injections on ``container`` with :class:`MockInjection`
    objects if their name is in ``names``.

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
        from nameko.standalone.rpc import rpc_proxy as standalone_rpc_proxy

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

        with standalone_rpc_proxy('conversionservice', config) as proxy:
            proxy.cm_to_inches(100)

        # assert that the injection was called as expected
        math.divide.assert_called_once_with(100, 2.54)

    """
    replacements = OrderedDict()

    for name in names:
        maybe_factory = getattr(container.service_cls, name, None)
        if isinstance(maybe_factory, DependencyFactory):
            factory = maybe_factory
            dependency = get_dependency(container, factory.dep_cls, name=name)
            if is_injection_provider(dependency):
                replacements[dependency] = MockInjection(name)

    for dependency, replacement in replacements.items():
        container.dependencies.remove(dependency)
        container.dependencies.add(replacement)

    # if only one name was provided, return any replacement directly
    # otherwise return a generator
    injections = (replacement.injection
                  for replacement in replacements.values())
    if len(names) == 1:
        return next(injections, None)
    return injections


def remove_entrypoints(container, *names):
    """ Remove the entrypoints from ``container`` if if their name is in
    ``names``.

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

    To remove the rpc entrypoint from "foo"::

        remove_entrypoints(container, "foo")

    To remove both the rpc and the event_handler entrypoints from "bar"::

        remove_entrypoints(container, "bar")

    Note that it is not possible to remove entrypoints individually.
    """
    dependencies = []

    for name in names:
        entrypoint_method = getattr(container.service_cls, name, None)
        entrypoint_factories = getattr(
            entrypoint_method, ENTRYPOINT_PROVIDERS_ATTR, tuple())
        for factory in entrypoint_factories:
            dependency = get_dependency(container, factory.dep_cls,
                                        name=name)
            dependencies.append(dependency)

    for dependency in dependencies:
        container.dependencies.remove(dependency)
