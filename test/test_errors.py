# start a runner with a service that errors. does it hang? or stop?
# how do we get an individual servicecontainer to blow up?
import eventlet
from mock import patch
import pytest


from nameko.exceptions import RemoteError
from nameko.events import EventDispatcher
from nameko.rpc import rpc, get_rpc_consumer, RpcConsumer


class ExampleError(Exception):
    pass


class ExampleService(object):

    dispatch = EventDispatcher()

    @rpc
    def task(self):
        return "task_result"

    @rpc
    def broken(self):
        raise ExampleError("broken")


def test_error_in_worker(container_factory, rabbit_config,
                         service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    proxy = service_proxy_factory(container, "exampleservice")

    with pytest.raises(RemoteError) as exc_info:
        proxy.broken()
    assert exc_info.value.exc_type == "ExampleError"


def test_handle_result_error(container_factory, rabbit_config,
                             service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    proxy = service_proxy_factory(container, "exampleservice")

    rpc_consumer = get_rpc_consumer(container.ctx, RpcConsumer)
    with patch.object(rpc_consumer, 'handle_result') as handle_result:
        err = "error in call_result"
        handle_result.side_effect = Exception(err)

        eventlet.spawn(proxy.task)

        with eventlet.Timeout(1):
            with pytest.raises(Exception) as exc_info:
                container.wait()
            assert exc_info.value.message == err


@pytest.mark.parametrize("method_name",
                         ["call_setup", "call_result", "call_teardown"])
def test_dependency_call_lifecycle_errors(container_factory, rabbit_config,
                                          service_proxy_factory, method_name):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    proxy = service_proxy_factory(container, "exampleservice")

    dependency = next(iter(container.dependencies.attributes))
    with patch.object(dependency, method_name) as method:
        err = "error in {}".format(method_name)
        method.side_effect = Exception(err)

        eventlet.spawn(proxy.task)

        with eventlet.Timeout(1):
            with pytest.raises(Exception) as exc_info:
                container.wait()
            assert exc_info.value.message == err


def test_runner_dies_on_container_error():
    pass
    # TODO
