from mock import Mock, call
import pytest

from nameko.containers import WorkerContext
from nameko.constants import PARENT_CALLS_CONFIG_KEY
from nameko.events import event_handler, EventDispatcher
from nameko.rpc import rpc, RpcProxy
from nameko.testing.services import entrypoint_waiter
from nameko.testing.utils import (
    get_container, worker_context_factory, DummyProvider)
from nameko.testing.services import entrypoint_hook


def get_logging_worker_context(stack_request):
    class LoggingWorkerContext(WorkerContext):
        def __init__(self, container, service, entrypoint, args=None,
                     kwargs=None, data=None):
            parent_stack = data.get('call_id_stack') if data else None
            stack_request(parent_stack)
            super(LoggingWorkerContext, self).__init__(
                container, service, entrypoint, args, kwargs, data
            )
    return LoggingWorkerContext


@pytest.mark.usefixtures("predictable_call_ids")
def test_worker_context_gets_stack(container_factory):
    context_cls = worker_context_factory()

    class FooService(object):
        name = 'baz'

    container = container_factory(FooService, {})
    service = FooService()

    context = context_cls(container, service, DummyProvider("bar"))
    assert context.call_id == 'baz.bar.0'
    assert context.call_id_stack == ['baz.bar.0']
    assert context.parent_call_stack == []

    # Build stack
    context = context_cls(container, service, DummyProvider("foo"),
                          data={'call_id_stack': context.call_id_stack})
    assert context.call_id == 'baz.foo.1'
    assert context.call_id_stack == ['baz.bar.0', 'baz.foo.1']

    # Long stack
    many_ids = [str(i) for i in range(10)]
    context = context_cls(container, service, DummyProvider("long"),
                          data={'call_id_stack': many_ids})
    expected = many_ids + ['baz.long.2']
    assert context.call_id_stack == expected


@pytest.mark.usefixtures("predictable_call_ids")
def test_short_call_stack(container_factory):
    context_cls = worker_context_factory()

    class FooService(object):
        name = 'baz'

    container = container_factory(FooService, {PARENT_CALLS_CONFIG_KEY: 1})
    service = FooService()

    # Trim stack
    many_ids = [str(i) for i in range(100)]
    context = context_cls(container, service, DummyProvider("long"),
                          data={'call_id_stack': many_ids})
    assert context.call_id_stack == ['99', 'baz.long.0']


def test_call_id_stack(rabbit_config, predictable_call_ids, runner_factory):
    child_do_called = Mock()

    stack_request = Mock()
    LoggingWorkerContext = get_logging_worker_context(stack_request)

    class Child(object):
        name = 'child'

        @rpc
        def child_do(self):
            child_do_called()
            return 1

    class Parent(object):
        name = "parent"

        child_service = RpcProxy('child')

        @rpc
        def parent_do(self):
            return self.child_service.child_do()

    class Grandparent(object):
        name = "grandparent"

        parent_service = RpcProxy('parent')

        @rpc
        def grandparent_do(self):
            return self.parent_service.parent_do()

    runner = runner_factory(rabbit_config)
    runner.add_service(Child, LoggingWorkerContext)
    runner.add_service(Parent, LoggingWorkerContext)
    runner.add_service(Grandparent, LoggingWorkerContext)
    runner.start()

    container = get_container(runner, Grandparent)
    with entrypoint_hook(container, "grandparent_do") as grandparent_do:
        assert grandparent_do() == 1

    # Check child is called
    child_do_called.assert_called_with()
    assert child_do_called.call_count == 1

    # Check IDs were requested
    assert predictable_call_ids.call_count == 3

    # Check call ID stack persisted over RPC
    stack_request.assert_has_calls([
        call(None),
        call(['grandparent.grandparent_do.0']),
        call(['grandparent.grandparent_do.0', 'parent.parent_do.1']),
    ])


def test_call_id_over_events(rabbit_config, predictable_call_ids,
                             runner_factory):
    one_called = Mock()
    two_called = Mock()

    stack_request = Mock()
    LoggingWorkerContext = get_logging_worker_context(stack_request)

    class EventListeningServiceOne(object):
        name = "listener_one"

        @event_handler('event_raiser', 'hello')
        def hello(self, name):
            one_called()

    class EventListeningServiceTwo(object):
        name = "listener_two"

        @event_handler('event_raiser', 'hello')
        def hello(self, name):
            two_called()

    class EventRaisingService(object):
        name = "event_raiser"
        dispatch = EventDispatcher()

        @rpc
        def say_hello(self):
            self.dispatch('hello', self.name)

    runner = runner_factory(rabbit_config)
    runner.add_service(EventListeningServiceOne, LoggingWorkerContext)
    runner.add_service(EventListeningServiceTwo, LoggingWorkerContext)
    runner.add_service(EventRaisingService, LoggingWorkerContext)
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
    stack_request.assert_has_calls([
        call(None),
        call(['event_raiser.say_hello.0']),
        call(['event_raiser.say_hello.0']),
    ])
