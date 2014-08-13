import json
import sys

from mock import ANY
import pytest

from nameko.dependencies import (
    InjectionProvider, DependencyFactory, injection, entrypoint)
from nameko.exceptions import RemoteError
from nameko.rpc import RpcProvider
from nameko.standalone.rpc import RpcProxy
from nameko.testing.utils import wait_for_worker_idle


worker_result_called = []


@pytest.fixture(autouse=True)
def reset():
    del worker_result_called[:]


class CollectorInjection(InjectionProvider):
    """ InjectionProvider that collects worker results
    """
    def acquire_injection(self, worker_ctx):
        pass

    def worker_result(self, worker_ctx, res, exc_info):
        worker_result_called.append((res, exc_info))


@injection
def result_collector():
    return DependencyFactory(CollectorInjection)


class CustomRpcProvider(RpcProvider):
    """ RpcProvider subclass that verifies `result` can be serialized to json,
    and changes the `result` and `exc_info` accordingly.
    """
    def handle_result(self, message, worker_ctx, result, exc_info):
        try:
            json.dumps(result)
        except Exception:
            result = "something went wrong"
            exc_info = sys.exc_info()

        return super(CustomRpcProvider, self).handle_result(
            message, worker_ctx, result, exc_info)


@entrypoint
def custom_rpc():
    return DependencyFactory(CustomRpcProvider)


class ExampleService(object):

    collector = result_collector()

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

    with RpcProxy('exampleservice', rabbit_config) as proxy:

        assert proxy.echo("hello") == "hello"
        with pytest.raises(RemoteError) as exc:
            proxy.unserializable()
        assert "is not JSON serializable" in exc.value.message

    wait_for_worker_idle(container)

    # verify CollectorInjection sees values returned from `handle_result`
    assert worker_result_called == [
        ("hello", None),
        ("something went wrong", (TypeError, ANY, ANY)),
    ]
