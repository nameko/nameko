import itertools
import time

import eventlet
import pytest
from mock import Mock, call

from nameko.events import event_handler
from nameko.exceptions import ExtensionNotFound, MethodNotFound
from nameko.extensions import DependencyProvider
from nameko.rpc import ServiceRpc, rpc
from nameko.standalone.events import event_dispatcher
from nameko.standalone.rpc import ServiceRpcClient
from nameko.testing.services import (
    entrypoint_hook, entrypoint_waiter, once, replace_dependencies,
    restrict_entrypoints, worker_factory
)
from nameko.testing.utils import get_container
from nameko.testing.waiting import wait_for_call


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
        except Exception:  # pragma: no cover
            pass


class Service(object):
    name = "service"

    a = ServiceRpc("service_a")
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
    b = ServiceRpc("service_b")

    @rpc
    def remote_method(self, value):
        res = "{}-a".format(value)
        return self.b.remote_method(res)


class ServiceB(object):

    name = "service_b"
    c = ServiceRpc("service_c")

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


@pytest.mark.usefixtures("rabbit_config")
def test_entrypoint_hook(runner_factory):

    service_classes = (Service, ServiceA, ServiceB, ServiceC)
    runner = runner_factory(*service_classes)
    runner.start()

    service_container = get_container(runner, Service)

    event_payload = "msg"
    with entrypoint_hook(service_container, 'handle') as handle:
        with entrypoint_waiter(service_container, 'handle'):
            handle(event_payload)
    handle_event.assert_called_once_with(event_payload)


@pytest.mark.usefixtures("rabbit_config")
def test_entrypoint_hook_with_return(runner_factory):

    service_classes = (Service, ServiceA, ServiceB, ServiceC)
    runner = runner_factory(*service_classes)
    runner.start()

    service_container = get_container(runner, Service)

    with entrypoint_hook(service_container, 'working') as working:
        assert working("value") == "value-a-b-c"

    with entrypoint_hook(service_container, 'broken') as broken:
        with pytest.raises(ExampleError):
            broken("value")


@pytest.mark.usefixtures("rabbit_config")
@pytest.mark.parametrize("context_data",
                         [{'language': 'en'}, {'language': 'fr'}])
def test_entrypoint_hook_context_data(container_factory, context_data):

    container = container_factory(Service)
    container.start()

    method = 'get_language'
    with entrypoint_hook(container, method, context_data) as get_language:
        assert get_language() == context_data['language']


@pytest.mark.usefixtures("rabbit_config")
def test_entrypoint_hook_dependency_not_found(container_factory):

    container = container_factory(Service)
    container.start()

    method = 'nonexistent_method'

    with pytest.raises(ExtensionNotFound):
        with entrypoint_hook(container, method):
            pass    # pragma: no cover


@pytest.mark.usefixtures("rabbit_config")
def test_entrypoint_hook_container_dying(container_factory):
    class DependencyError(Exception):
        pass

    class BadDependency(DependencyProvider):
        def worker_setup(self, worker_ctx):
            raise DependencyError("Boom")

    class BadService(Service):
        bad = BadDependency()

    container = container_factory(BadService)
    container.start()

    with pytest.raises(DependencyError):
        with entrypoint_hook(container, 'working') as call:
            call()


def test_entrypoint_hook_timeout(container_factory, rabbit_config):

    class Service:
        name = 'service'

        @rpc
        def long_task(self):
            time.sleep(0.1)

    container = container_factory(Service, rabbit_config)
    container.start()

    with pytest.raises(entrypoint_waiter.Timeout) as exc_info:
        with entrypoint_hook(container, 'long_task', timeout=0.01) as call:
            call()

    assert str(exc_info.value) == (
        'Timeout on service.long_task after 0.01 seconds'
    )


def test_worker_factory():

    class Service(object):
        name = "service"
        foo_client = ServiceRpc("foo_service")
        bar_client = ServiceRpc("bar_service")

    class OtherService(object):
        pass

    # simplest case, no overrides
    instance = worker_factory(Service)
    assert isinstance(instance, Service)
    assert isinstance(instance.foo_client, Mock)
    assert isinstance(instance.bar_client, Mock)

    # no dependencies to replace
    instance = worker_factory(OtherService)
    assert isinstance(instance, OtherService)

    # override specific dependency
    bar_dependency = object()
    instance = worker_factory(Service, bar_client=bar_dependency)
    assert isinstance(instance, Service)
    assert isinstance(instance.foo_client, Mock)
    assert instance.bar_client is bar_dependency

    # non-applicable dependency
    with pytest.raises(ExtensionNotFound):
        worker_factory(Service, nonexist=object())


@pytest.mark.usefixtures("rabbit_config")
def test_replace_dependencies_kwargs(container_factory):

    class Service(object):
        name = "service"
        foo_client = ServiceRpc("foo_service")
        bar_client = ServiceRpc("bar_service")
        baz_client = ServiceRpc("baz_service")

        @rpc
        def method(self, arg):
            self.foo_client.remote_method(arg)

    class FakeDependency(object):
        def __init__(self):
            self.processed = []

        def remote_method(self, arg):
            self.processed.append(arg)

    container = container_factory(Service)

    # customise a single dependency
    fake_foo_client = FakeDependency()
    replace_dependencies(container, foo_client=fake_foo_client)
    assert 2 == len([dependency for dependency in container.extensions
                     if isinstance(dependency, ServiceRpc)])

    # customise multiple dependencies
    res = replace_dependencies(container, bar_client=Mock(), baz_client=Mock())
    assert list(res) == []

    # verify that container.extensions doesn't include an ServiceRpc anymore
    assert all([not isinstance(dependency, ServiceRpc)
                for dependency in container.extensions])

    container.start()

    # verify that the fake dependency collected calls
    msg = "msg"
    with ServiceRpcClient("service") as service_client:
        service_client.method(msg)

    assert fake_foo_client.processed == [msg]


@pytest.mark.usefixtures("rabbit_config")
def test_replace_dependencies_args(container_factory):

    class Service(object):
        name = "service"
        foo_client = ServiceRpc("foo_service")
        bar_client = ServiceRpc("bar_service")
        baz_client = ServiceRpc("baz_service")

        @rpc
        def method(self, arg):
            self.foo_client.remote_method(arg)

    container = container_factory(Service)

    # replace a single dependency
    foo_client = replace_dependencies(container, "foo_client")

    # replace multiple dependencies
    replacements = replace_dependencies(container, "bar_client", "baz_client")
    assert len([x for x in replacements]) == 2

    # verify that container.extensions doesn't include an ServiceRpc anymore
    assert all([not isinstance(dependency, ServiceRpc)
                for dependency in container.extensions])

    container.start()

    # verify that the mock dependency collects calls
    msg = "msg"
    with ServiceRpcClient("service") as service_client:
        service_client.method(msg)

    foo_client.remote_method.assert_called_once_with(msg)


@pytest.mark.usefixtures("rabbit_config")
def test_replace_dependencies_args_and_kwargs(container_factory):
    class Service(object):
        name = "service"
        foo_client = ServiceRpc("foo_service")
        bar_client = ServiceRpc("bar_service")
        baz_client = ServiceRpc("baz_service")

        @rpc
        def method(self, arg):
            self.foo_client.remote_method(arg)
            self.bar_client.bar()
            self.baz_client.baz()

    class FakeDependency(object):
        def __init__(self):
            self.processed = []

        def remote_method(self, arg):
            self.processed.append(arg)

    container = container_factory(Service)

    fake_foo_client = FakeDependency()
    mock_bar_client, mock_baz_client = replace_dependencies(
        container, 'bar_client', 'baz_client', foo_client=fake_foo_client
    )

    # verify that container.extensions doesn't include an ServiceRpc anymore
    assert all([not isinstance(dependency, ServiceRpc)
                for dependency in container.extensions])

    container.start()

    # verify that the fake dependency collected calls
    msg = "msg"
    with ServiceRpcClient("service") as service_client:
        service_client.method(msg)

    assert fake_foo_client.processed == [msg]
    assert mock_bar_client.bar.call_count == 1
    assert mock_baz_client.baz.call_count == 1


@pytest.mark.usefixtures("rabbit_config")
def test_replace_dependencies_in_both_args_and_kwargs_error(
    container_factory
):
    class Service(object):
        name = "service"
        foo_client = ServiceRpc("foo_service")
        bar_client = ServiceRpc("bar_service")
        baz_client = ServiceRpc("baz_service")

    container = container_factory(Service)

    with pytest.raises(RuntimeError) as exc:
        replace_dependencies(
            container, 'bar_client', 'foo_client', foo_client='foo'
        )
    assert "Cannot replace the same dependency" in str(exc)


@pytest.mark.usefixtures("rabbit_config")
def test_replace_non_dependency(container_factory):

    class Service(object):
        name = "service"
        client = ServiceRpc("foo_service")

        @rpc
        def method(self):
            pass  # pragma: no cover

    container = container_factory(Service)

    # error if dependency doesn't exit
    with pytest.raises(ExtensionNotFound):
        replace_dependencies(container, "nonexist")

    # error if dependency is not an dependency
    with pytest.raises(ExtensionNotFound):
        replace_dependencies(container, "method")


@pytest.mark.usefixtures("rabbit_config")
def test_replace_dependencies_container_already_started(container_factory):

    class Service(object):
        name = "service"
        client = ServiceRpc("foo_service")

    container = container_factory(Service)
    container.start()

    with pytest.raises(RuntimeError):
        replace_dependencies(container, "client")


@pytest.mark.usefixtures("rabbit_config")
def test_restrict_entrypoints(container_factory):

    method_called = Mock()

    class Service(object):
        name = "service"

        @rpc
        @once("assert not seen")
        def handler_one(self, arg):
            method_called(arg)  # pragma: no cover

        @event_handler('srcservice', 'eventtype')
        def handler_two(self, msg):
            method_called(msg)

    container = container_factory(Service)

    # disable the entrypoints on handler_one
    restrict_entrypoints(container, "handler_two")
    container.start()

    # verify the rpc entrypoint on handler_one is disabled
    with ServiceRpcClient("service") as service_client:
        with pytest.raises(MethodNotFound) as exc_info:
            service_client.handler_one("msg")
        assert str(exc_info.value) == "handler_one"

    # dispatch an event to handler_two
    msg = "msg"
    dispatch = event_dispatcher()

    with entrypoint_waiter(container, 'handler_two'):
        dispatch('srcservice', 'eventtype', msg)

    # method_called should have exactly one call, derived from the event
    # handler and not from the disabled @once entrypoint
    method_called.assert_called_once_with(msg)


@pytest.mark.usefixtures("rabbit_config")
def test_restrict_nonexistent_entrypoint(container_factory):

    class Service(object):
        name = "service"

        @rpc
        def method(self, arg):
            pass  # pragma: no cover

    container = container_factory(Service)

    with pytest.raises(ExtensionNotFound):
        restrict_entrypoints(container, "nonexist")


@pytest.mark.usefixtures("rabbit_config")
def test_restrict_entrypoint_container_already_started(container_factory):

    class Service(object):
        name = "service"

        @rpc
        def method(self, arg):
            pass  # pragma: no cover

    container = container_factory(Service)
    container.start()

    with pytest.raises(RuntimeError):
        restrict_entrypoints(container, "method")


@pytest.mark.usefixtures("rabbit_config")
def test_entrypoint_waiter(container_factory):
    container = container_factory(Service)
    container.start()

    dispatch = event_dispatcher()
    with entrypoint_waiter(container, 'handle'):
        dispatch('srcservice', 'eventtype', "")


@pytest.mark.usefixtures("rabbit_config")
def test_entrypoint_waiter_result(container_factory):

    class Service(object):
        name = "service"

        @event_handler('srcservice', 'eventtype')
        def handle_event(self, msg):
            return msg.upper()

    container = container_factory(Service)
    container.start()

    dispatch = event_dispatcher()
    with entrypoint_waiter(container, 'handle_event') as result:
        dispatch('srcservice', 'eventtype', "msg")

    res = result.get()
    assert res == "MSG"


@pytest.mark.usefixtures("rabbit_config")
def test_entrypoint_waiter_with_callback(container_factory):

    class Service(object):
        name = "service"

        @event_handler('srcservice', 'eventtype')
        def handle_event(self, msg):
            return msg

    container = container_factory(Service)
    container.start()

    results = []

    def cb(worker_ctx, res, exc_info):
        results.append((res, exc_info))
        return len(results) == 2

    dispatch = event_dispatcher()
    with entrypoint_waiter(container, 'handle_event', callback=cb):
        dispatch('srcservice', 'eventtype', "msg1")
        dispatch('srcservice', 'eventtype', "msg2")

    assert results == [("msg1", None), ("msg2", None)]


@pytest.mark.usefixtures("rabbit_config")
def test_entrypoint_waiter_wait_for_specific_result(
    container_factory, spawn_thread
):

    class Service(object):
        name = "service"

        @event_handler('srcservice', 'eventtype')
        def handle_event(self, msg):
            return msg

    container = container_factory(Service)
    container.start()

    target = 5

    def cb(worker_ctx, res, exc_info):
        return res == target

    def increment_forever():
        dispatch = event_dispatcher()
        for count in itertools.count():
            dispatch('srcservice', 'eventtype', count)
            time.sleep()  # force yield

    with entrypoint_waiter(container, 'handle_event', callback=cb) as result:
        spawn_thread(increment_forever)

    assert result.get() == target


@pytest.mark.usefixtures("rabbit_config")
def test_entrypoint_waiter_wait_until_called_with_argument(
    container_factory, spawn_thread
):

    class Service(object):
        name = "service"

        @event_handler('srcservice', 'eventtype')
        def handle_event(self, msg):
            return msg

    container = container_factory(Service)
    container.start()

    target = 5

    def cb(worker_ctx, res, exc_info):
        return worker_ctx.args == (target,)

    def increment_forever():
        dispatch = event_dispatcher()
        for count in itertools.count():
            dispatch('srcservice', 'eventtype', count)
            time.sleep()  # force yield

    with entrypoint_waiter(container, 'handle_event', callback=cb) as result:
        spawn_thread(increment_forever)

    assert result.get() == target


@pytest.mark.usefixtures("rabbit_config")
def test_entrypoint_waiter_wait_until_raises(
    container_factory, spawn_thread
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

    container = container_factory(Service)
    container.start()

    def cb(worker_ctx, res, exc_info):
        return exc_info is not None

    def increment_forever():
        dispatch = event_dispatcher()
        for count in itertools.count():
            dispatch('srcservice', 'eventtype', count)
            time.sleep()  # force yield

    with entrypoint_waiter(container, 'handle_event', callback=cb) as result:
        spawn_thread(increment_forever)

    with pytest.raises(TooMuch):
        result.get()


@pytest.mark.usefixtures("rabbit_config")
def test_entrypoint_waiter_wait_until_stops_raising(
    container_factory, spawn_thread
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

    container = container_factory(Service)
    container.start()

    def cb(worker_ctx, res, exc_info):
        return exc_info is None

    def increment_forever():
        dispatch = event_dispatcher()
        for count in itertools.count():
            dispatch('srcservice', 'eventtype', count)
            time.sleep()  # force yield

    with entrypoint_waiter(container, 'handle_event', callback=cb) as result:
        spawn_thread(increment_forever)

    assert result.get() == threshold


@pytest.mark.usefixtures("rabbit_config")
def test_entrypoint_waiter_timeout(container_factory):
    container = container_factory(Service)
    container.start()

    with pytest.raises(entrypoint_waiter.Timeout) as exc_info:
        with entrypoint_waiter(container, 'handle', timeout=0.01):
            pass
    assert str(exc_info.value) == (
        "Timeout on service.handle after 0.01 seconds")


@pytest.mark.usefixtures("rabbit_config")
def test_entrypoint_waiter_bad_entrypoint(container_factory):
    container = container_factory(Service)

    with pytest.raises(RuntimeError) as exc:
        with entrypoint_waiter(container, "unknown"):
            pass  # pragma: no cover
    assert 'has no entrypoint' in str(exc)


@pytest.mark.usefixtures("rabbit_config")
def test_entrypoint_waiter_nested(container_factory):

    class Service(object):
        name = "service"

        @event_handler('srcservice', 'eventtype1')
        def handle_event1(self, msg):
            handle_event(1)

        @event_handler('srcservice', 'eventtype2')
        def handle_event2(self, msg):
            handle_event(2)

    container = container_factory(Service)
    container.start()

    dispatch = event_dispatcher()
    with entrypoint_waiter(container, 'handle_event1'):
        with entrypoint_waiter(container, 'handle_event2'):
            dispatch('srcservice', 'eventtype1', "")
            dispatch('srcservice', 'eventtype2', "")

    assert call(1) in handle_event.call_args_list
    assert call(2) in handle_event.call_args_list


@pytest.mark.usefixtures("rabbit_config")
def test_entrypoint_waiter_duplicate(container_factory):

    class Service(object):
        name = "service"

        @event_handler('srcservice', 'eventtype')
        def handle_event(self, msg):
            handle_event(msg)

    container = container_factory(Service)
    container.start()

    dispatch = event_dispatcher()
    with entrypoint_waiter(container, 'handle_event'):
        with entrypoint_waiter(container, 'handle_event'):
            dispatch('srcservice', 'eventtype', "msg")

    assert handle_event.call_args_list == [call("msg")]


@pytest.mark.usefixtures("rabbit_config")
def test_entrypoint_waiter_result_teardown_race(
    container_factory, counter
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

    container = container_factory(Service)
    container.start()

    def wait_for_two_calls(worker_ctx, res, exc_info):
        return counter.count() > 1

    dispatch = event_dispatcher()
    with entrypoint_waiter(container, 'handle', callback=wait_for_two_calls):

        # dispatch the first message
        # wait until teardown has happened
        with wait_for_call(TrackingDependency, 'worker_teardown'):
            dispatch('srcservice', 'eventtype', "msg")

        assert tracker.worker_teardown.call_count == 1
        assert tracker.worker_result.call_count == 1
        assert tracker.handle.call_count == 1

        # dispatch the second event
        dispatch('srcservice', 'eventtype', "msg")

    # we should wait for the second teardown to complete before exiting
    # the entrypoint waiter
    assert tracker.worker_teardown.call_count == 2
    assert tracker.worker_result.call_count == 2
    assert tracker.handle.call_count == 2
