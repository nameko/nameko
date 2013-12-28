"""
Utilities for testing nameko services.
"""

from contextlib import contextmanager
import inspect

from eventlet import event
from mock import Mock

from nameko.dependencies import (
    get_entrypoint_providers, DependencyFactory, InjectionProvider)


@contextmanager
def entrypoint_hook(container, name, context_data=None):
    """ Yield a function providing an entrypoint into a hosted service.

    The yielded function may be called as if it were the bare method defined
    in the service class. Intended to be used as an integration testing
    utility.

    Usage
    =====

    To verify that ServiceX and ServiceY are compatible, make an integration
    test that checks their interaction::

        class ServiceX(object):
            name = "service_x"

            y = rpc_proxy("service_y")

            @rpc
            def remote_method(self, value):
                res = "{}-x".format(value)
                return self.y.append_identifier(res)


        class ServiceY(object):
            name = "service_y"

            @rpc
            def append_identifier(self, value):
                return "{}-y".format(value)

    Given a ServiceRunner hosting these services::

        # get the container hosting ServiceX
        container = get_container(runner, ServiceX)

        # using an ``entrypoint_hook`` into "remote_method", verify the
        # interaction between the two hosted services
        with entrypoint_hook(container, "remote_method") as entrypoint:
            assert entrypoint("value") == "value-x-y"

    """
    provider = next(prov for prov in get_entrypoint_providers(container)
                    if prov.name == name)

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

        class Service(object):
            math = rpc_proxy("math_service")

            @rpc
            def add(self, x, y):
                return self.math.add(x, y)

            @rpc
            def subtract(self, x, y):
                return self.math.subtract(x, y)

    Use the ``instance_factory`` to create an unhosted instance of ``Service``
    with its injections replaced by Mock objects::

        service = instance_factory(Service)

    Nameko's entrypoints do not modify the service methods, so they can be
    called directly on an unhosted instance. The injection Mocks can be used
    as any other Mock object, so a complete unit test for Service may look
    like this::

        # create instance
        service = instance_factory(Service)

        # mock out "math" service
        service.math.add.side_effect = lambda x, y: x + y
        service.math.subtract.side_effect = lambda x, y: x - y

        # test add business logic
        assert service.add(3, 4) == 7
        service.math.add.assert_called_once_with(3, 4)

        # test subtract business logic
        assert service.subtract(5, 2) == 3
        service.math.subtract.assert_called_once_with(5, 2)

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
