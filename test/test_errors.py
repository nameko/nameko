# start a runner with a service that errors. does it hang? or stop?
# how do we get an individual servicecontainer to blow up?
import eventlet
from mock import patch, call, ANY
import pytest


from nameko.exceptions import RemoteError
from nameko.events import event_dispatcher, EventDispatcher
from nameko.rpc import rpc, rpc_proxy, RpcConsumer
from nameko.runners import ServiceRunner
from nameko.testing.utils import get_dependency


class ExampleError(Exception):
    pass


class ExampleService(object):

    dispatch = event_dispatcher()
    rpcproxy = rpc_proxy('exampleservice')

    @rpc
    def task(self):
        return "task_result"

    @rpc
    def proxy(self):
        self.rpcproxy.broken()

    @rpc
    def broken(self):
        raise ExampleError("broken")


@pytest.yield_fixture
def logger():
    with patch('nameko.containers._log') as logger:
        yield logger


def test_error_in_worker(container_factory, rabbit_config,
                         service_proxy_factory, logger):

    container = container_factory(ExampleService, rabbit_config)
    proxy = service_proxy_factory(container, "exampleservice")
    container.start()

    with pytest.raises(RemoteError) as exc_info:
        proxy.broken()
    assert exc_info.value.exc_type == "ExampleError"
    assert logger.error.call_args == call('error handling worker %s: %s', ANY,
                                          ANY, exc_info=True)
    assert not container._died.ready()


def test_error_in_remote_worker(container_factory, rabbit_config,
                                service_proxy_factory, logger):

    container = container_factory(ExampleService, rabbit_config)
    proxy = service_proxy_factory(container, "exampleservice")
    container.start()

    with pytest.raises(RemoteError) as exc_info:
        proxy.proxy()
    assert exc_info.value.exc_type == "RemoteError"
    assert logger.error.call_args == call('error handling worker %s: %s', ANY,
                                          "RemoteError", exc_info=True)
    assert not container._died.ready()


def test_handle_result_error(container_factory, rabbit_config,
                             service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    proxy = service_proxy_factory(container, "exampleservice")
    container.start()

    rpc_consumer = get_dependency(container, RpcConsumer)
    with patch.object(rpc_consumer, 'handle_result') as handle_result:
        err = "error in worker_result"
        handle_result.side_effect = Exception(err)

        eventlet.spawn(proxy.task)

        with eventlet.Timeout(1):
            with pytest.raises(Exception) as exc_info:
                container.wait()
            assert exc_info.value.message == err


@pytest.mark.parametrize("method_name",
                         ["worker_setup", "worker_result", "worker_teardown"])
def test_dependency_call_lifecycle_errors(container_factory, rabbit_config,
                                          service_proxy_factory, method_name):

    container = container_factory(ExampleService, rabbit_config)
    proxy = service_proxy_factory(container, "exampleservice")
    container.start()

    dependency = get_dependency(container, EventDispatcher)
    with patch.object(dependency, method_name) as method:
        err = "error in {}".format(method_name)
        method.side_effect = Exception(err)

        eventlet.spawn(proxy.task)

        with eventlet.Timeout(1):
            with pytest.raises(Exception) as exc_info:
                container.wait()
            assert exc_info.value.message == err


def test_runner_catches_container_errors(container_factory, rabbit_config,
                                         service_proxy_factory):

    runner = ServiceRunner(rabbit_config)
    runner.add_service(ExampleService)

    import mock
    from nameko.utils import SpawningProxy

    # we can't actually start the container until we've had a chance to
    # attach service_proxy, but we can't access the containers until we
    # call runner.start
    with mock.patch('nameko.runners.SpawningProxy'):
        runner.start()

    container = runner.containers[0]
    proxy = service_proxy_factory(container, "exampleservice")

    # actually start the containers (previously bypassed with mock)
    SpawningProxy(runner.containers).start()

    rpc_consumer = get_dependency(container, RpcConsumer)
    with patch.object(rpc_consumer, 'handle_result') as handle_result:
        exception = Exception("error")
        handle_result.side_effect = exception

        eventlet.spawn(proxy.task)

        with pytest.raises(Exception) as exc_info:
            runner.wait()
        assert exc_info.value == exception
