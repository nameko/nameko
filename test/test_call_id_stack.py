import pytest
from mock import Mock, call

from nameko.constants import PARENT_CALLS_CONFIG_KEY
from nameko.containers import WorkerContext
from nameko.events import EventDispatcher, event_handler
from nameko.extensions import DependencyProvider
from nameko.rpc import RpcProxy, rpc
from nameko.testing.services import entrypoint_hook, entrypoint_waiter
from nameko.testing.utils import DummyProvider, get_container


@pytest.mark.usefixtures("predictable_call_ids")
def test_worker_context_gets_stack(container_factory):

    class FooService(object):
        name = 'baz'

    container = container_factory(FooService, {})
    service = FooService()

    context = WorkerContext(container, service, DummyProvider("bar"))
    assert context.call_id == 'baz.bar.0'
    assert context.call_id_stack == ['baz.bar.0']

    # Build stack
    context = WorkerContext(
        container, service, DummyProvider("foo"),
        data={'call_id_stack': context.call_id_stack}
    )
    assert context.call_id == 'baz.foo.1'
    assert context.call_id_stack == ['baz.bar.0', 'baz.foo.1']

    # Long stack
    many_ids = [str(i) for i in range(10)]
    context = WorkerContext(
        container, service, DummyProvider("long"),
        data={'call_id_stack': many_ids}
    )
    expected = many_ids + ['baz.long.2']
    assert context.call_id_stack == expected


@pytest.mark.usefixtures("predictable_call_ids")
def test_short_call_stack(container_factory):

    class FooService(object):
        name = 'baz'

    container = container_factory(FooService, {PARENT_CALLS_CONFIG_KEY: 1})
    service = FooService()

    # Trim stack
    many_ids = [str(i) for i in range(100)]
    context = WorkerContext(
        container, service, DummyProvider("long"),
        data={'call_id_stack': many_ids}
    )
    assert context.call_id_stack == ['99', 'baz.long.0']


@pytest.fixture
def tracker():
    return Mock()


@pytest.fixture
def stack_logger(tracker):

    class StackLogger(DependencyProvider):

        def worker_setup(self, worker_ctx):
            tracker(worker_ctx.call_id_stack)

    return StackLogger


def test_call_id_stack(
    rabbit_config, predictable_call_ids, runner_factory, stack_logger, tracker
):
    StackLogger = stack_logger

    class Child(object):
        name = 'child'

        stack_logger = StackLogger()

        @rpc
        def method(self):
            return 1

    class Parent(object):
        name = "parent"

        stack_logger = StackLogger()
        child_service = RpcProxy('child')

        @rpc
        def method(self):
            return self.child_service.method()

    class Grandparent(object):
        name = "grandparent"

        stack_logger = StackLogger()
        parent_service = RpcProxy('parent')

        @rpc
        def method(self):
            return self.parent_service.method()

    runner = runner_factory(rabbit_config)
    runner.add_service(Child)
    runner.add_service(Parent)
    runner.add_service(Grandparent)
    runner.start()

    container = get_container(runner, Grandparent)
    with entrypoint_hook(container, "method") as grandparent_method:
        assert grandparent_method() == 1

    # Check IDs were requested
    assert predictable_call_ids.call_count == 3

    # Check call ID stack persisted over RPC
    assert tracker.call_args_list == [
        call(['grandparent.method.0']),
        call(['grandparent.method.0', 'parent.method.1']),
        call(['grandparent.method.0', 'parent.method.1', 'child.method.2']),
    ]


def test_call_id_over_events(
    rabbit_config, predictable_call_ids, runner_factory, stack_logger, tracker
):
    StackLogger = stack_logger

    one_called = Mock()
    two_called = Mock()

    class EventListeningServiceOne(object):
        name = "listener_one"

        stack_logger = StackLogger()

        @event_handler('event_raiser', 'hello')
        def hello(self, name):
            one_called()

    class EventListeningServiceTwo(object):
        name = "listener_two"

        stack_logger = StackLogger()

        @event_handler('event_raiser', 'hello')
        def hello(self, name):
            two_called()

    class EventRaisingService(object):
        name = "event_raiser"
        dispatch = EventDispatcher()

        stack_logger = StackLogger()

        @rpc
        def say_hello(self):
            self.dispatch('hello', self.name)

    runner = runner_factory(rabbit_config)
    runner.add_service(EventListeningServiceOne)
    runner.add_service(EventListeningServiceTwo)
    runner.add_service(EventRaisingService)
    runner.start()

    container = get_container(runner, EventRaisingService)
    listener1 = get_container(runner, EventListeningServiceOne)
    listener2 = get_container(runner, EventListeningServiceTwo)
    with entrypoint_hook(container, "say_hello") as say_hello:
        waiter1 = entrypoint_waiter(listener1, 'hello')
        waiter2 = entrypoint_waiter(listener2, 'hello')
        with waiter1, waiter2:
            say_hello()

    assert predictable_call_ids.call_count == 3

    # order of event handlers and dependencies is non-deterministic,
    # so there are four permutations of valid call stacks
    possible_call_lists = (
        [
            call(['event_raiser.say_hello.0']),
            call(['event_raiser.say_hello.0', 'listener_one.hello.1']),
            call(['event_raiser.say_hello.0', 'listener_two.hello.2']),
        ],
        [
            call(['event_raiser.say_hello.0']),
            call(['event_raiser.say_hello.0', 'listener_one.hello.2']),
            call(['event_raiser.say_hello.0', 'listener_two.hello.1']),
        ],
        [
            call(['event_raiser.say_hello.0']),
            call(['event_raiser.say_hello.0', 'listener_two.hello.1']),
            call(['event_raiser.say_hello.0', 'listener_one.hello.2']),
        ],
        [
            call(['event_raiser.say_hello.0']),
            call(['event_raiser.say_hello.0', 'listener_two.hello.2']),
            call(['event_raiser.say_hello.0', 'listener_one.hello.1']),
        ]
    )
    assert tracker.call_args_list in possible_call_lists


class TestImmediateParentCallId(object):

    def test_with_parent(self, mock_container):

        mock_container.service_name = "foo"

        service = Mock()
        entrypoint = DummyProvider("bar")
        context_data = {
            'call_id_stack': [
                'parent.method.1',
                'parent.method.2',
                'parent.method.3'
            ]
        }

        worker_ctx = WorkerContext(
            mock_container, service, entrypoint, data=context_data
        )

        assert worker_ctx.immediate_parent_call_id == "parent.method.3"

    def test_without_parent(self, mock_container):

        mock_container.service_name = "foo"

        service = Mock()
        entrypoint = DummyProvider("bar")
        context_data = {}

        worker_ctx = WorkerContext(
            mock_container, service, entrypoint, data=context_data
        )

        assert worker_ctx.immediate_parent_call_id is None


class TestOriginCallId(object):

    def test_with_origin(self, mock_container):

        mock_container.service_name = "foo"

        service = Mock()
        entrypoint = DummyProvider("bar")
        context_data = {
            'call_id_stack': [
                'parent.method.1',
                'parent.method.2',
                'parent.method.3'
            ]
        }

        worker_ctx = WorkerContext(
            mock_container, service, entrypoint, data=context_data
        )

        assert worker_ctx.origin_call_id == "parent.method.1"

    def test_without_origin(self, mock_container):

        mock_container.service_name = "foo"

        service = Mock()
        entrypoint = DummyProvider("bar")
        context_data = {}

        worker_ctx = WorkerContext(
            mock_container, service, entrypoint, data=context_data
        )

        assert worker_ctx.origin_call_id is None
