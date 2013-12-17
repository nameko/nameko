"""
Utilities for unit testing services.
"""

import inspect

from mock import Mock

from nameko.dependencies import DependencyFactory, InjectionProvider


def instance_factory(service_cls, **injections):
    """ Return an instance of ``service_cls`` with its injected dependencies
    replaced with Mock objects, or as given in ``injections``.

    Usage:

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
