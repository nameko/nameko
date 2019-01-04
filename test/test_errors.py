# start a runner with a service that errors. does it hang? or stop?
# how do we get an individual servicecontainer to blow up?
import pytest
from mock import ANY, call, patch

from nameko.events import EventDispatcher
from nameko.exceptions import RemoteError
from nameko.rpc import RpcConsumer, ServiceRpc, rpc
from nameko.standalone.rpc import ServiceRpcClient
from nameko.testing.services import entrypoint_hook
from nameko.testing.utils import get_container, get_extension


class ExampleError(Exception):
    pass


class ExampleService(object):
    name = "exampleservice"

    dispatch = EventDispatcher()
    exampleservice_rpc = ServiceRpc('exampleservice')

    @rpc
    def task(self):
        return "task_result"

    @rpc
    def broken(self):
        raise ExampleError("broken")

    @rpc(expected_exceptions=ExampleError)
    def bad(self):
        raise ExampleError("bad")

    @rpc
    def proxy(self, method, *args):
        """ Makes RPC calls to ``method`` on itself, so we can test handling
        of errors in remote services.
        """
        getattr(self.exampleservice_rpc, method)(*args)


class SecondService(object):
    name = "secondservice"

    @rpc
    def task(self):
        return 'task_result'  # pragma: no cover


@pytest.mark.usefixtures("rabbit_config")
def test_error_in_worker(container_factory):

    container = container_factory(ExampleService)
    container.start()

    with patch('nameko.containers._log') as logger:
        with entrypoint_hook(container, "broken") as broken:
            with pytest.raises(ExampleError):
                broken()
    assert not container._died.ready()

    assert logger.exception.call_args == call(
        'error handling worker %s: %s', ANY, ANY)


@pytest.mark.usefixtures("rabbit_config")
def test_expected_error_in_worker(container_factory):

    container = container_factory(ExampleService)
    container.start()

    with patch('nameko.containers._log') as logger:
        with entrypoint_hook(container, "bad") as bad:
            with pytest.raises(ExampleError):
                bad()
    assert not container._died.ready()

    assert logger.warning.call_args == call(
        '(expected) error handling worker %s: %s', ANY, ANY, exc_info=True)


@pytest.mark.usefixtures("rabbit_config")
def test_error_in_remote_worker(container_factory):

    container = container_factory(ExampleService)
    container.start()

    with entrypoint_hook(container, "proxy") as proxy:
        with pytest.raises(RemoteError) as exc_info:
            proxy("broken")
    assert exc_info.value.exc_type == "ExampleError"
    assert not container._died.ready()


@pytest.mark.usefixtures("rabbit_config")
def test_handle_result_error(container_factory):

    container = container_factory(ExampleService)
    container.start()

    rpc_consumer = get_extension(container, RpcConsumer)
    with patch.object(
            rpc_consumer, 'handle_result', autospec=True
    ) as handle_result:
        err = "error in handle_result"
        handle_result.side_effect = Exception(err)

        # use a standalone rpc client to call exampleservice.task()
        with ServiceRpcClient("exampleservice") as client:
            # client.task() will hang forever because it generates an error
            client.task.call_async()

        with pytest.raises(Exception) as exc_info:
            container.wait()

        assert str(exc_info.value) == err


@pytest.mark.usefixtures("rabbit_config")
@pytest.mark.parametrize("method_name",
                         ["worker_setup", "worker_result", "worker_teardown"])
def test_dependency_call_lifecycle_errors(
        container_factory, rabbit_config, method_name):

    container = container_factory(ExampleService)
    container.start()

    dependency = get_extension(container, EventDispatcher)
    with patch.object(dependency, method_name, autospec=True) as method:
        err = "error in {}".format(method_name)
        method.side_effect = Exception(err)

        # use a standalone rpc client to call exampleservice.task()
        with ServiceRpcClient("exampleservice") as client:
            # client.task() will hang forever because it generates an error
            client.task.call_async()

        # verify that the error bubbles up to container.wait()
        with pytest.raises(Exception) as exc_info:
            container.wait()
        assert str(exc_info.value) == err


@pytest.mark.usefixtures("rabbit_config")
def test_runner_catches_container_errors(runner_factory):

    runner = runner_factory(ExampleService)
    runner.start()

    container = get_container(runner, ExampleService)

    rpc_consumer = get_extension(container, RpcConsumer)
    with patch.object(
            rpc_consumer, 'handle_result', autospec=True) as handle_result:
        exception = Exception("error")
        handle_result.side_effect = exception

        # use a standalone rpc client to call exampleservice.task()
        with ServiceRpcClient("exampleservice") as client:
            # client.task() will hang forever because it generates an error
            # in the remote container (so never receives a response).
            client.task.call_async()

        # verify that the error bubbles up to runner.wait()
        with pytest.raises(Exception) as exc_info:
            runner.wait()
        assert exc_info.value == exception


@pytest.mark.usefixtures("rabbit_config")
def test_graceful_stop_on_one_container_error(runner_factory):

    runner = runner_factory(ExampleService, SecondService)
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

            # use a standalone rpc client to call exampleservice.task()
            with ServiceRpcClient("exampleservice") as client:
                # client.task() will hang forever because it generates an error
                # in the remote container (so never receives a response).
                client.task.call_async()

            # verify that the error bubbles up to runner.wait()
            with pytest.raises(Exception) as exc_info:
                runner.wait()
            assert exc_info.value == exception

            # Check that the second service was stopped due to the first
            # service being killed
            stop.assert_called_once_with()
