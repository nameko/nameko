from eventlet import Timeout
from eventlet.event import Event as EventletEvent
from mock import Mock, call
import pytest

from nameko.containers import WorkerContext
from nameko.events import event_handler, event_dispatcher, Event as NamekoEvent
from nameko.rpc import rpc, rpc_proxy
from nameko.runners import ServiceRunner
from nameko.testing.once import once
from nameko.testing.utils import (
    wait_for_call, predictable_call_ids, worker_context_factory)


def get_logging_worker_context(stack_request):
    class LoggingWorkerContext(WorkerContext):
        def __init__(self, *args, **kwargs):
            parent_stack = kwargs.get('parent_call_stack')
            stack_request(parent_stack)
            super(LoggingWorkerContext, self).__init__(*args, **kwargs)
    return LoggingWorkerContext


def test_worker_context_gets_stack(container_factory):
    with predictable_call_ids():
        context_cls = worker_context_factory()

        class FooService(object):
            name = 'baz'

        container = container_factory(FooService, {})
        service = FooService()

        context = context_cls(container, service, "bar")
        assert context.call_id == 'baz.bar.0'
        assert context.call_id_stack == ['baz.bar.0']
        assert context.parent_call_stack == []

        # Build stack
        context = context_cls(container, service, "foo",
                              parent_call_stack=context.call_id_stack)
        assert context.call_id == 'baz.foo.1'
        assert context.call_id_stack == ['baz.bar.0', 'baz.foo.1']

        # Trim stack
        many_ids = [str(i) for i in xrange(100)]
        context = context_cls(container, service, "long",
                              parent_call_stack=many_ids)
        assert context.call_id_stack == ['99', 'baz.long.2']


@pytest.mark.usefixtures("reset_rabbit")
def test_call_id_stack(rabbit_config):
    with predictable_call_ids() as get_id:
        wait_to_go = EventletEvent()
        e = EventletEvent()
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
            child_service = rpc_proxy('child')

            @rpc
            def parent_do(self):
                r = self.child_service.child_do()
                return r

        class Grandparent(object):
            parent_service = rpc_proxy('parent')

            @once()
            def grandparent_do(self):
                wait_to_go.wait()
                r = self.parent_service.parent_do()
                e.send(True)
                return r

        runner = ServiceRunner(config=rabbit_config)
        runner.add_service(Child, LoggingWorkerContext)
        runner.add_service(Parent, LoggingWorkerContext)
        runner.add_service(Grandparent, LoggingWorkerContext)
        wait_to_go.send(runner.start())

        with Timeout(30):
            e.wait()
        runner.stop()

        # Check child is called
        child_do_called.assert_called_with()
        assert child_do_called.call_count == 1

        # Check IDs were requested
        assert get_id.call_count == 3

        # Check call ID stack persisted over RPC
        stack_request.assert_has_calls([
            call(None),
            call(['grandparent.grandparent_do.0']),
            call(['grandparent.grandparent_do.0', 'parent.parent_do.1']),
        ])


@pytest.mark.usefixtures("reset_rabbit")
def test_call_id_over_events(rabbit_config):
    with predictable_call_ids() as get_id:
        one_called = Mock()
        two_called = Mock()
        wait_to_go = EventletEvent()

        stack_request = Mock()
        LoggingWorkerContext = get_logging_worker_context(stack_request)

        class HelloEvent(NamekoEvent):
            type = "hello"

        class EventListeningServiceOne(object):
            @event_handler('event_raiser', 'hello')
            def hello(self, name):
                one_called()

        class EventListeningServiceTwo(object):
            @event_handler('event_raiser', 'hello')
            def hello(self, name):
                two_called()

        class EventRaisingService(object):
            name = "event_raiser"
            dispatch = event_dispatcher()

            @once()
            def say_hello(self):
                wait_to_go.wait()
                self.dispatch(HelloEvent(self.name))

        runner = ServiceRunner(config=rabbit_config)
        runner.add_service(EventListeningServiceOne, LoggingWorkerContext)
        runner.add_service(EventListeningServiceTwo, LoggingWorkerContext)
        runner.add_service(EventRaisingService, LoggingWorkerContext)
        wait_to_go.send(runner.start())

        with wait_for_call(5, one_called), wait_for_call(5, two_called):
            runner.stop()

        assert get_id.call_count == 3
        stack_request.assert_has_calls([
            call(None),
            call(['event_raiser.say_hello.0']),
            call(['event_raiser.say_hello.0']),
        ])
