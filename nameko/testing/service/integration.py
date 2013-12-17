"""
Utilities for integration testing services.
"""

from contextlib import contextmanager

from eventlet import event

from nameko.dependencies import get_entrypoint_providers


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
