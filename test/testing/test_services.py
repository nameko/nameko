import itertools
import time

import eventlet
import pytest
from mock import Mock, call

from nameko.events import event_handler
from nameko.exceptions import ExtensionNotFound, MethodNotFound
from nameko.extensions import DependencyProvider
from nameko.rpc import RpcProxy, rpc
from nameko.standalone.events import event_dispatcher
from nameko.standalone.rpc import ServiceRpcProxy
from nameko.testing.services import (
    entrypoint_hook, entrypoint_waiter, once, replace_dependencies,
    restrict_entrypoints, worker_factory)
from nameko.testing.utils import get_container


class LanguageReporter(DependencyProvider):
    """ Return the language given in the worker context data
    """
    def get_dependency(self, worker_ctx):
        def get_language():
            return worker_ctx.data['language']
        return get_language


handle_event = Mock()


@pytest.fixture
def counter():

    class Counter(object):
        value = 0

        def count(self):
            self.value += 1
            return self.value

    return Counter()


@pytest.yield_fixture(autouse=True)
def reset_mock():
    yield
    handle_event.reset_mock()


@pytest.yield_fixture
def spawn_thread():

    threads = []

    def spawn(fn, *args):
        """ Spawn a new thread to execute `fn(*args)`.

        The thread will be killed at test teardown if it's still running.
        """
        threads.append(eventlet.spawn(fn, *args))

    yield spawn

    for gt in threads:
        try:
            gt.kill()
        except Exception:
            pass


class Service(object):
    name = "service"

    a = RpcProxy("service_a")
    language = LanguageReporter()

    @rpc
    def working(self, value):
        return self.a.remote_method(value)

    @rpc
    def broken(self, value):
        raise ExampleError('broken')

    @event_handler('srcservice', 'eventtype')
    def handle(self, msg):
        handle_event(msg)

    @rpc
    def get_language(self):
        return self.language()


class ServiceA(object):

    name = "service_a"
    b = RpcProxy("service_b")

    @rpc
    def remote_method(self, value):
        res = "{}-a".format(value)
        return self.b.remote_method(res)


class ServiceB(object):

    name = "service_b"
    c = RpcProxy("service_c")

    @rpc
    def remote_method(self, value):
        res = "{}-b".format(value)
        return self.c.remote_method(res)


class ServiceC(object):

    name = "service_c"

    @rpc
    def remote_method(self, value):
        return "{}-c".format(value)


class ExampleError(Exception):
    pass


def test_entrypoint_hook(runner_factory, rabbit_config):

    service_classes = (Service, ServiceA, ServiceB, ServiceC)
    runner = runner_factory(rabbit_config, *service_classes)
    runner.start()

    service_container = get_container(runner, Service)

    event_payload = "msg"
    with entrypoint_hook(service_container, 'handle') as handle:
        with entrypoint_waiter(service_container, 'handle'):
            handle(event_payload)
    handle_event.assert_called_once_with(event_payload)


def test_entrypoint_hook_with_return(runner_factory, rabbit_config):

    service_classes = (Service, ServiceA, ServiceB, ServiceC)
    runner = runner_factory(rabbit_config, *service_classes)
    runner.start()

    service_container = get_container(runner, Service)

    with entrypoint_hook(service_container, 'working') as working:
        assert working("value") == "value-a-b-c"

    with entrypoint_hook(service_container, 'broken') as broken:
        with pytest.raises(ExampleError):
            broken("value")


@pytest.mark.parametrize("context_data",
                         [{'language': 'en'}, {'language': 'fr'}])
def test_entrypoint_hook_context_data(container_factory, rabbit_config,
                                      context_data):

    container = container_factory(Service, rabbit_config)
    container.start()

    method = 'get_language'
    with entrypoint_hook(container, method, context_data) as get_language:
        assert get_language() == context_data['language']


def test_entrypoint_hook_dependency_not_found(container_factory,
                                              rabbit_config):

    container = container_factory(Service, rabbit_config)
    container.start()

    method = 'nonexistent_method'

    with pytest.raises(ExtensionNotFound):
        with entrypoint_hook(container, method):
            pass


def test_entrypoint_hook_container_dying(container_factory, rabbit_config):
    class DependencyError(Exception):
        pass

    class BadDependency(DependencyProvider):
        def worker_setup(self, worker_ctx):
            raise DependencyError("Boom")

    class BadService(Service):
        bad = BadDependency()

    container = container_factory(BadService, rabbit_config)
    container.start()

    with pytest.raises(DependencyError):
        with entrypoint_hook(container, 'working') as call:
            call()


def test_worker_factory():

    class Service(object):
        name = "service"
        foo_proxy = RpcProxy("foo_service")
        bar_proxy = RpcProxy("bar_service")

    class OtherService(object):
        pass

    # simplest case, no overrides
    instance = worker_factory(Service)
    assert isinstance(instance, Service)
    assert isinstance(instance.foo_proxy, Mock)
    assert isinstance(instance.bar_proxy, Mock)

    # no dependencies to replace
    instance = worker_factory(OtherService)
    assert isinstance(instance, OtherService)

    # override specific dependency
    bar_dependency = object()
    instance = worker_factory(Service, bar_proxy=bar_dependency)
    assert isinstance(instance, Service)
    assert isinstance(instance.foo_proxy, Mock)
    assert instance.bar_proxy is bar_dependency

    # non-applicable dependency
    with pytest.raises(ExtensionNotFound):
        worker_factory(Service, nonexist=object())


def test_replace_dependencies_kwargs(container_factory, rabbit_config):

    class Service(object):
        name = "service"
        foo_proxy = RpcProxy("foo_service")
        bar_proxy = RpcProxy("bar_service")
        baz_proxy = RpcProxy("baz_service")

        @rpc
        def method(self, arg):
            self.foo_proxy.remote_method(arg)

    class FakeDependency(object):
        def __init__(self):
            self.processed = []

        def remote_method(self, arg):
            self.processed.append(arg)

    container = container_factory(Service, rabbit_config)

    # customise a single dependency
    fake_foo_proxy = FakeDependency()
    replace_dependencies(container, foo_proxy=fake_foo_proxy)
    assert 2 == len([dependency for dependency in container.extensions
                     if isinstance(dependency, RpcProxy)])

    # customise multiple dependencies
    res = replace_dependencies(container, bar_proxy=Mock(), baz_proxy=Mock())
    assert list(res) == []

    # verify that container.extensions doesn't include an RpcProxy anymore
    assert all([not isinstance(dependency, RpcProxy)
                for dependency in container.extensions])

    container.start()

    # verify that the fake dependency collected calls
    msg = "msg"
    with ServiceRpcProxy("service", rabbit_config) as service_proxy:
        service_proxy.method(msg)

    assert fake_foo_proxy.processed == [msg]


def test_replace_dependencies_args(container_factory, rabbit_config):

    class Service(object):
        name = "service"
        foo_proxy = RpcProxy("foo_service")
        bar_proxy = RpcProxy("bar_service")
        baz_proxy = RpcProxy("baz_service")

        @rpc
        def method(self, arg):
            self.foo_proxy.remote_method(arg)

    container = container_factory(Service, rabbit_config)

    # replace a single dependency
    foo_proxy = replace_dependencies(container, "foo_proxy")

    # replace multiple dependencies
    replacements = replace_dependencies(container, "bar_proxy", "baz_proxy")
    assert len([x for x in replacements]) == 2

    # verify that container.extensions doesn't include an RpcProxy anymore
    assert all([not isinstance(dependency, RpcProxy)
                for dependency in container.extensions])

    container.start()

    # verify that the mock dependency collects calls
    msg = "msg"
    with ServiceRpcProxy("service", rabbit_config) as service_proxy:
        service_proxy.method(msg)

    foo_proxy.remote_method.assert_called_once_with(msg)


def test_replace_dependencies_args_and_kwargs(container_factory,
                                              rabbit_config):
    class Service(object):
        name = "service"
        foo_proxy = RpcProxy("foo_service")
        bar_proxy = RpcProxy("bar_service")
        baz_proxy = RpcProxy("baz_service")

        @rpc
        def method(self, arg):
            self.foo_proxy.remote_method(arg)
            self.bar_proxy.bar()
            self.baz_proxy.baz()

    class FakeDependency(object):
        def __init__(self):
            self.processed = []

        def remote_method(self, arg):
            self.processed.append(arg)

    container = container_factory(Service, rabbit_config)

    fake_foo_proxy = FakeDependency()
    mock_bar_proxy, mock_baz_proxy = replace_dependencies(
        container, 'bar_proxy', 'baz_proxy', foo_proxy=fake_foo_proxy
    )

    # verify that container.extensions doesn't include an RpcProxy anymore
    assert all([not isinstance(dependency, RpcProxy)
                for dependency in container.extensions])

    container.start()

    # verify that the fake dependency collected calls
    msg = "msg"
    with ServiceRpcProxy("service", rabbit_config) as service_proxy:
        service_proxy.method(msg)

    assert fake_foo_proxy.processed == [msg]
    assert mock_bar_proxy.bar.call_count == 1
    assert mock_baz_proxy.baz.call_count == 1


def test_replace_dependencies_in_both_args_and_kwargs_error(container_factory,
                                                            rabbit_config):
    class Service(object):
        name = "service"
        foo_proxy = RpcProxy("foo_service")
        bar_proxy = RpcProxy("bar_service")
        baz_proxy = RpcProxy("baz_service")

    container = container_factory(Service, rabbit_config)

    with pytest.raises(RuntimeError) as exc:
        replace_dependencies(
            container, 'bar_proxy', 'foo_proxy', foo_proxy='foo'
        )
    assert "Cannot replace the same dependency" in str(exc)


def test_replace_non_dependency(container_factory, rabbit_config):

    class Service(object):
        name = "service"
        proxy = RpcProxy("foo_service")

        @rpc
        def method(self):
            pass

    container = container_factory(Service, rabbit_config)

    # error if dependency doesn't exit
    with pytest.raises(ExtensionNotFound):
        replace_dependencies(container, "nonexist")

    # error if dependency is not an dependency
    with pytest.raises(ExtensionNotFound):
        replace_dependencies(container, "method")


def test_replace_dependencies_container_already_started(container_factory,
                                                        rabbit_config):

    class Service(object):
        name = "service"
        proxy = RpcProxy("foo_service")

    container = container_factory(Service, rabbit_config)
    container.start()

    with pytest.raises(RuntimeError):
        replace_dependencies(container, "proxy")


def test_restrict_entrypoints(container_factory, rabbit_config):

    method_called = Mock()

    class Service(object):
        name = "service"

        @rpc
        @once("assert not seen")
        def handler_one(self, arg):
            method_called(arg)

        @event_handler('srcservice', 'eventtype')
        def handler_two(self, msg):
            method_called(msg)

    container = container_factory(Service, rabbit_config)

    # disable the entrypoints on handler_one
    restrict_entrypoints(container, "handler_two")
    container.start()

    # verify the rpc entrypoint on handler_one is disabled
    with ServiceRpcProxy("service", rabbit_config) as service_proxy:
        with pytest.raises(MethodNotFound) as exc_info:
            service_proxy.handler_one("msg")
        assert str(exc_info.value) == "handler_one"

    # dispatch an event to handler_two
    msg = "msg"
    dispatch = event_dispatcher(rabbit_config)

    with entrypoint_waiter(container, 'handler_two'):
        dispatch('srcservice', 'eventtype', msg)

    # method_called should have exactly one call, derived from the event
    # handler and not from the disabled @once entrypoint
    method_called.assert_called_once_with(msg)


def test_restrict_nonexistent_entrypoint(container_factory, rabbit_config):

    class Service(object):
        name = "service"

        @rpc
        def method(self, arg):
            pass

    container = container_factory(Service, rabbit_config)

    with pytest.raises(ExtensionNotFound):
        restrict_entrypoints(container, "nonexist")


def test_restrict_entrypoint_container_already_started(container_factory,
                                                       rabbit_config):

    class Service(object):
        name = "service"

        @rpc
        def method(self, arg):
            pass

    container = container_factory(Service, rabbit_config)
    container.start()

    with pytest.raises(RuntimeError):
        restrict_entrypoints(container, "method")


def test_entrypoint_waiter(container_factory, rabbit_config):
    container = container_factory(Service, rabbit_config)
    container.start()

    dispatch = event_dispatcher(rabbit_config)
    with entrypoint_waiter(container, 'handle'):
        dispatch('srcservice', 'eventtype', "")


def test_entrypoint_waiter_result(container_factory, rabbit_config):

    class Service(object):
        name = "service"

        @event_handler('srcservice', 'eventtype')
        def handle_event(self, msg):
            return msg.upper()

    container = container_factory(Service, rabbit_config)
    container.start()

    dispatch = event_dispatcher(rabbit_config)
    with entrypoint_waiter(container, 'handle_event') as result:
        dispatch('srcservice', 'eventtype', "msg")

    res = result.get()
    assert res == "MSG"


def test_entrypoint_waiter_with_callback(container_factory, rabbit_config):

    class Service(object):
        name = "service"

        @event_handler('srcservice', 'eventtype')
        def handle_event(self, msg):
            return msg

    container = container_factory(Service, rabbit_config)
    container.start()

    results = []

    def cb(worker_ctx, res, exc_info):
        results.append((res, exc_info))
        return len(results) == 2

    dispatch = event_dispatcher(rabbit_config)
    with entrypoint_waiter(container, 'handle_event', callback=cb):
        dispatch('srcservice', 'eventtype', "msg1")
        dispatch('srcservice', 'eventtype', "msg2")

    assert results == [("msg1", None), ("msg2", None)]


def test_entrypoint_waiter_wait_for_specific_result(
    container_factory, rabbit_config, spawn_thread
):

    class Service(object):
        name = "service"

        @event_handler('srcservice', 'eventtype')
        def handle_event(self, msg):
            return msg

    container = container_factory(Service, rabbit_config)
    container.start()

    target = 5

    def cb(worker_ctx, res, exc_info):
        return res == target

    def increment_forever():
        dispatch = event_dispatcher(rabbit_config)
        for count in itertools.count():
            dispatch('srcservice', 'eventtype', count)
            time.sleep()  # force yield

    with entrypoint_waiter(container, 'handle_event', callback=cb) as result:
        spawn_thread(increment_forever)

    assert result.get() == target


def test_entrypoint_waiter_wait_until_called_with_argument(
    container_factory, rabbit_config, spawn_thread
):

    class Service(object):
        name = "service"

        @event_handler('srcservice', 'eventtype')
        def handle_event(self, msg):
            return msg

    container = container_factory(Service, rabbit_config)
    container.start()

    target = 5

    def cb(worker_ctx, res, exc_info):
        return worker_ctx.args == (target,)

    def increment_forever():
        dispatch = event_dispatcher(rabbit_config)
        for count in itertools.count():
            dispatch('srcservice', 'eventtype', count)
            time.sleep()  # force yield

    with entrypoint_waiter(container, 'handle_event', callback=cb) as result:
        spawn_thread(increment_forever)

    assert result.get() == target


def test_entrypoint_waiter_wait_until_raises(
    container_factory, rabbit_config, spawn_thread
):
    threshold = 5

    class TooMuch(Exception):
        pass

    class Service(object):
        name = "service"

        @event_handler('srcservice', 'eventtype')
        def handle_event(self, msg):
            if msg > threshold:
                raise TooMuch(msg)
            return msg

    container = container_factory(Service, rabbit_config)
    container.start()

    def cb(worker_ctx, res, exc_info):
        return exc_info is not None

    def increment_forever():
        dispatch = event_dispatcher(rabbit_config)
        for count in itertools.count():
            dispatch('srcservice', 'eventtype', count)
            time.sleep()  # force yield

    with entrypoint_waiter(container, 'handle_event', callback=cb) as result:
        spawn_thread(increment_forever)

    with pytest.raises(TooMuch):
        result.get()


def test_entrypoint_waiter_wait_until_stops_raising(
    container_factory, rabbit_config, spawn_thread
):
    threshold = 5

    class NotEnough(Exception):
        pass

    class Service(object):
        name = "service"

        @event_handler('srcservice', 'eventtype')
        def handle_event(self, msg):
            if msg < threshold:
                raise NotEnough(msg)
            return msg

    container = container_factory(Service, rabbit_config)
    container.start()

    def cb(worker_ctx, res, exc_info):
        return exc_info is None

    def increment_forever():
        dispatch = event_dispatcher(rabbit_config)
        for count in itertools.count():
            dispatch('srcservice', 'eventtype', count)
            time.sleep()  # force yield

    with entrypoint_waiter(container, 'handle_event', callback=cb) as result:
        spawn_thread(increment_forever)

    assert result.get() == threshold


def test_entrypoint_waiter_timeout(container_factory, rabbit_config):
    container = container_factory(Service, rabbit_config)
    container.start()

    with pytest.raises(entrypoint_waiter.Timeout) as exc_info:
        with entrypoint_waiter(container, 'handle', timeout=0.01):
            pass
    assert str(exc_info.value) == (
        "Timeout on service.handle after 0.01 seconds")


def test_entrypoint_waiter_bad_entrypoint(container_factory, rabbit_config):
    container = container_factory(Service, rabbit_config)

    with pytest.raises(RuntimeError) as exc:
        with entrypoint_waiter(container, "unknown"):
            pass
    assert 'has no entrypoint' in str(exc)


def test_entrypoint_waiter_nested(container_factory, rabbit_config):

    class Service(object):
        name = "service"

        @event_handler('srcservice', 'eventtype1')
        def handle_event1(self, msg):
            handle_event(1)

        @event_handler('srcservice', 'eventtype2')
        def handle_event2(self, msg):
            handle_event(2)

    container = container_factory(Service, rabbit_config)
    container.start()

    dispatch = event_dispatcher(rabbit_config)
    with entrypoint_waiter(container, 'handle_event1'):
        with entrypoint_waiter(container, 'handle_event2'):
            dispatch('srcservice', 'eventtype1', "")
            dispatch('srcservice', 'eventtype2', "")

    assert call(1) in handle_event.call_args_list
    assert call(2) in handle_event.call_args_list


def test_entrypoint_waiter_duplicate(container_factory, rabbit_config):

    class Service(object):
        name = "service"

        @event_handler('srcservice', 'eventtype')
        def handle_event(self, msg):
            handle_event(msg)

    container = container_factory(Service, rabbit_config)
    container.start()

    dispatch = event_dispatcher(rabbit_config)
    with entrypoint_waiter(container, 'handle_event'):
        with entrypoint_waiter(container, 'handle_event'):
            dispatch('srcservice', 'eventtype', "msg")

    assert handle_event.call_args_list == [call("msg")]


def test_entrypoint_waiter_result_teardown_race(
    container_factory, rabbit_config, counter
):
    tracker = Mock()

    class TrackingDependency(DependencyProvider):

        def worker_result(self, worker_ctx, res, exc_info):
            tracker.worker_result()

        def worker_teardown(self, worker_ctx):
            tracker.worker_teardown()

    class Service(object):
        name = "service"

        tracker = TrackingDependency()

        @event_handler('srcservice', 'eventtype')
        def handle(self, msg):
            tracker.handle(msg)

    container = container_factory(Service, rabbit_config)
    container.start()

    def wait_for_two_calls(worker_ctx, res, exc_info):
        return counter.count() > 1

    dispatch = event_dispatcher(rabbit_config)
    with entrypoint_waiter(container, 'handle', callback=wait_for_two_calls):

        # dispatch the first message
        dispatch('srcservice', 'eventtype', "msg")

        # wait until teardown has fired at least once
        while tracker.worker_teardown.call_count == 0:
            time.sleep(.1)

        # dispatch the second event
        dispatch('srcservice', 'eventtype', "msg")

    # we should wait for the second teardown to complete before exiting
    # the entrypoint waiter
    assert tracker.worker_teardown.call_count == 2
    assert tracker.worker_result.call_count == 2
    assert tracker.handle.call_count == 2
