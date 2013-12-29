"""
Utilities for testing nameko services.
"""

from contextlib import contextmanager
import inspect

from eventlet import event
from mock import Mock

from nameko.dependencies import (
    get_entrypoint_providers, DependencyFactory, InjectionProvider)
from nameko.exceptions import DependencyNotFound


@contextmanager
def entrypoint_hook(container, name, context_data=None):
    """ Yield a function providing an entrypoint into a hosted service.

    The yielded function may be called as if it were the bare method defined
    in the service class. Intended to be used as an integration testing
    utility.

    Usage
    =====

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

    Usage
    =====

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

    Providing Injections
    --------------------

    The ``**injections`` kwargs to ``instance_factory`` can be used to provide
    a replacement injection instead of a Mock. For example, to unit test a
    service against a real database:

    .. literalinclude:: examples/testing/unit_test_with_provided_injection.py

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
