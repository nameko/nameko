import pytest

from mock import Mock

from nameko.exceptions import RemoteError
from nameko.rpc import rpc, Service


class ExampleError(Exception):
    pass


class ExampleService(object):

    @rpc
    def task_a(self, *args, **kwargs):
        print "task_a", args, kwargs
        return "result_a"

    @rpc
    def task_b(self, *args, **kwargs):
        print "task_b", args, kwargs
        return "result_b"

    @rpc
    def broken(self):
        raise ExampleError("broken")

    @rpc
    def echo(self, *args, **kwargs):
        return args, kwargs


# def test_exceptions(get_connection):
#     class Controller(object):
#         def test_method(self, context, **kwargs):
#             raise KeyError('foo')

#     srv = service.Service(
#         Controller, connection_factory=get_connection,
#         exchange='testrpc', topic='test', )
#     srv.start()
#     eventlet.sleep()

#     try:
#         test = TestProxy(get_connection, timeout=3).test
#         with pytest.raises(exceptions.RemoteError):
#             test.test_method()

#         with pytest.raises(exceptions.RemoteError):
#             test.test_method_does_not_exist()
#     finally:
#         srv.kill()


@pytest.fixture
def service_proxy_factory(request):
    def make_proxy(container, service_name):
        worker_ctx = Mock(srv_ctx=container.ctx)
        service_proxy = Service(service_name)
        proxy = service_proxy.acquire_injection(worker_ctx)
        return proxy
    return make_proxy


# test rpc proxy ...


def test_rpc_consumer_creates_single_consumer(container_factory, rabbit_config,
                                              rabbit_manager):
    container = container_factory(ExampleService, rabbit_config)
    container.start()

    vhost = rabbit_config['vhost']
    queues = rabbit_manager.get_queues(vhost)
    assert len(queues) == 1
    assert queues[0]['name'] == "rpc-exampleservice"


def test_rpc_args_kwargs(container_factory, rabbit_config,
                         service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    proxy = service_proxy_factory(container, "exampleservice")

    assert proxy.echo() == [[], {}]
    assert proxy.echo("a", "b") == [["a", "b"], {}]
    assert proxy.echo(foo="bar") == [[], {'foo': 'bar'}]
    assert proxy.echo("arg", kwarg="kwarg") == [["arg"], {'kwarg': 'kwarg'}]


def test_rpc_existing_method(container_factory, rabbit_config, rabbit_manager,
                             service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    proxy = service_proxy_factory(container, "exampleservice")

    assert proxy.task_a() == "result_a"
    assert proxy.task_b() == "result_b"


def test_rpc_missing_method(container_factory, rabbit_config, rabbit_manager,
                            service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    proxy = service_proxy_factory(container, "exampleservice")

    with pytest.raises(RemoteError) as exc_info:
        proxy.task_c()
    assert exc_info.value.exc_type == "MethodNotFound"


def test_rpc_broken_method(container_factory, rabbit_config,
                           rabbit_manager, service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    proxy = service_proxy_factory(container, "exampleservice")

    with pytest.raises(RemoteError) as exc_info:
        proxy.broken()
    assert exc_info.value.exc_type == "ExampleError"


# test failure in reply publish
# test reply-to and correlation-id correct
