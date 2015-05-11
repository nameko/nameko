import json
import sys

from mock import ANY
import pytest

from nameko.extensions import DependencyProvider
from nameko.exceptions import RemoteError
from nameko.rpc import Rpc
from nameko.standalone.rpc import ServiceRpcProxy
from nameko.testing.utils import wait_for_worker_idle


worker_result_called = []


@pytest.yield_fixture(autouse=True)
def reset():
    yield
    del worker_result_called[:]


class ResultCollector(DependencyProvider):
    """ DependencyProvider that collects worker results
    """
    def worker_result(self, worker_ctx, res, exc_info):
        worker_result_called.append((res, exc_info))


class CustomRpc(Rpc):
    """ Rpc subclass that verifies `result` can be serialized to json,
    and changes the `result` and `exc_info` accordingly.
    """
    def handle_result(self, message, worker_ctx, result, exc_info):
        try:
            json.dumps(result)
        except Exception:
            result = "something went wrong"
            exc_info = sys.exc_info()

        return super(CustomRpc, self).handle_result(
            message, worker_ctx, result, exc_info)


custom_rpc = CustomRpc.decorator


class ExampleService(object):
    name = "exampleservice"

    collector = ResultCollector()

    @custom_rpc
    def echo(self, arg):
        return arg

    @custom_rpc
    def unserializable(self):
        return object()


def test_handle_result(container_factory, rabbit_manager, rabbit_config):
    """ Verify that `handle_result` can modify the return values of the worker,
    such that other dependencies see the updated values.
    """
    container = container_factory(ExampleService, rabbit_config)
    container.start()

    with ServiceRpcProxy('exampleservice', rabbit_config) as proxy:

        assert proxy.echo("hello") == "hello"

        with pytest.raises(RemoteError) as exc:
            proxy.unserializable()
        assert "is not JSON serializable" in str(exc.value)

    wait_for_worker_idle(container)

    # verify ResultCollector sees values returned from `handle_result`
    assert worker_result_called == [
        ("hello", None),
        ("something went wrong", (TypeError, ANY, ANY)),
    ]
