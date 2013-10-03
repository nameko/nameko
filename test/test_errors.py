# start a runner with a service that errors. does it hang? or stop?
# how do we get an individual servicecontainer to blow up?
from mock import patch
import pytest


from nameko.exceptions import RemoteError
from nameko.rpc import rpc, get_rpc_consumer, RpcConsumer


class ExampleError(Exception):
    pass


class ExampleService(object):

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
        handle_result.side_effect = Exception("error")

        proxy.task()


def test_dependency_call_result_error():
    pass


def test_dependency_call_teardown_error():
    pass


def test_runner_dies_on_container_error():
    pass
