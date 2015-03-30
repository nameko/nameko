# start a runner with a service that errors. does it hang? or stop?
# how do we get an individual servicecontainer to blow up?
import eventlet
from mock import patch
import pytest


from nameko.exceptions import RemoteError
from nameko.events import EventDispatcher
from nameko.rpc import rpc, RpcProxy, RpcConsumer
from nameko.standalone.rpc import ServiceRpcProxy
from nameko.testing.utils import get_extension, get_container
from nameko.testing.services import entrypoint_hook


class ExampleError(Exception):
    pass


class ExampleService(object):
    name = "exampleservice"

    dispatch = EventDispatcher()
    rpcproxy = RpcProxy('exampleservice')

    @rpc
    def task(self):
        return "task_result"

    @rpc
    def broken(self):
        raise ExampleError("broken")

    @rpc
    def proxy(self, method, *args):
        """ Proxies RPC calls to ``method`` on itself, so we can test handling
        of errors in remote services.
        """
        getattr(self.rpcproxy, method)(*args)


class SecondService(object):
    name = "secondservice"

    @rpc
    def task(self):
        return 'task_result'


def test_error_in_worker(container_factory, rabbit_config):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    with entrypoint_hook(container, "broken") as broken:
        with pytest.raises(ExampleError):
            broken()
    assert not container._died.ready()


def test_error_in_remote_worker(container_factory, rabbit_config):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    with entrypoint_hook(container, "proxy") as proxy:
        with pytest.raises(RemoteError) as exc_info:
            proxy("broken")
    assert exc_info.value.exc_type == "ExampleError"
    assert not container._died.ready()


def test_handle_result_error(container_factory, rabbit_config):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    rpc_consumer = get_extension(container, RpcConsumer)
    with patch.object(
            rpc_consumer, 'handle_result', autospec=True) as handle_result:
        err = "error in handle_result"
        handle_result.side_effect = Exception(err)

        # use a standalone rpc proxy to call exampleservice.task()
        with ServiceRpcProxy("exampleservice", rabbit_config) as proxy:
            # proxy.task() will hang forever because it generates an error
            proxy.task.async()

        with eventlet.Timeout(1):
            with pytest.raises(Exception) as exc_info:
                container.wait()
            assert str(exc_info.value) == err


@pytest.mark.parametrize("method_name",
                         ["worker_setup", "worker_result", "worker_teardown"])
def test_dependency_call_lifecycle_errors(
        container_factory, rabbit_config, method_name):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    dependency = get_extension(container, EventDispatcher)
    with patch.object(dependency, method_name, autospec=True) as method:
        err = "error in {}".format(method_name)
        method.side_effect = Exception(err)

        # use a standalone rpc proxy to call exampleservice.task()
        with ServiceRpcProxy("exampleservice", rabbit_config) as proxy:
            # proxy.task() will hang forever because it generates an error
            proxy.task.async()

        # verify that the error bubbles up to container.wait()
        with eventlet.Timeout(1):
            with pytest.raises(Exception) as exc_info:
                container.wait()
            assert str(exc_info.value) == err


def test_runner_catches_container_errors(runner_factory, rabbit_config):

    runner = runner_factory(rabbit_config, ExampleService)
    runner.start()

    container = get_container(runner, ExampleService)

    rpc_consumer = get_extension(container, RpcConsumer)
    with patch.object(
            rpc_consumer, 'handle_result', autospec=True) as handle_result:
        exception = Exception("error")
        handle_result.side_effect = exception

        # use a standalone rpc proxy to call exampleservice.task()
        with ServiceRpcProxy("exampleservice", rabbit_config) as proxy:
            # proxy.task() will hang forever because it generates an error
            # in the remote container (so never receives a response).
            proxy.task.async()

        # verify that the error bubbles up to runner.wait()
        with pytest.raises(Exception) as exc_info:
            runner.wait()
        assert exc_info.value == exception


def test_graceful_stop_on_one_container_error(runner_factory, rabbit_config):

    runner = runner_factory(rabbit_config, ExampleService, SecondService)
    runner.start()

    container = get_container(runner, ExampleService)
    second_container = get_container(runner, SecondService)
    original_stop = second_container.stop
    with patch.object(second_container, 'stop', autospec=True,
                      wraps=original_stop) as stop:
        rpc_consumer = get_extension(container, RpcConsumer)
        with patch.object(
                rpc_consumer, 'handle_result', autospec=True) as handle_result:
            exception = Exception("error")
            handle_result.side_effect = exception

            # use a standalone rpc proxy to call exampleservice.task()
            with ServiceRpcProxy("exampleservice", rabbit_config) as proxy:
                # proxy.task() will hang forever because it generates an error
                # in the remote container (so never receives a response).
                proxy.task.async()

            # verify that the error bubbles up to runner.wait()
            with pytest.raises(Exception) as exc_info:
                runner.wait()
            assert exc_info.value == exception

            # Check that the second service was stopped due to the first
            # service being killed
            stop.assert_called_once_with()
