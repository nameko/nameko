from contextlib import contextmanager
from eventlet import Timeout
from eventlet.event import Event as EventletEvent
from mock import Mock, patch, call
import pytest

from nameko.containers import ServiceContainer, WorkerContext
from nameko.events import event_handler, event_dispatcher, Event as NamekoEvent
from nameko.once import once, OnceProvider
from nameko.rpc import rpc, rpc_proxy
from nameko.runners import ServiceRunner
from nameko.testing.utils import wait_for_call


class Increment(object):
    def __init__(self):
        self.i = -1

    def next(self):
        self.i += 1
        return str(self.i)


def get_log_id_container(stack_request):
    class LogIdContainer(ServiceContainer):
        def _prepare_call_id_stack(self, current_stack=None):
            stack_request(current_stack)
            return super(LogIdContainer, self).\
                _prepare_call_id_stack(current_stack)

    return LogIdContainer


@contextmanager
def int_ids():
    with patch('nameko.containers.new_call_id') as get_id:
        i = Increment()
        get_id.side_effect = i.next
        yield get_id


def test_service_container_gets_stack(rabbit_config):
    with int_ids() as get_id:
        stack_request = Mock()
        LogIdContainer = get_log_id_container(stack_request)

        class Service(object):
            pass

        container = LogIdContainer(Service, WorkerContext, rabbit_config)
        entry = OnceProvider()
        entry.bind('foobar', container)
        entry.prepare()
        entry.start()

        with wait_for_call(1, stack_request):
            entry.stop()

        stack_request.assert_called_once_with(None)
        assert get_id.call_count == 1


@pytest.mark.usefixtures("reset_rabbit")
def test_call_id_stack(rabbit_config):
    with int_ids() as get_id:
        wait_to_go = EventletEvent()
        e = EventletEvent()

        child_do_called = Mock()

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

        stack_request = Mock()
        LogIdContainer = get_log_id_container(stack_request)

        runner = ServiceRunner(config=rabbit_config,
                               container_cls=LogIdContainer)
        runner.add_service(Child)
        runner.add_service(Parent)
        runner.add_service(Grandparent)
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
            call(['0']),
            call(['1']),
        ])


@pytest.mark.usefixtures("reset_rabbit")
def test_call_id_over_events(rabbit_config):
    with int_ids() as get_id:
        one_called = Mock()
        two_called = Mock()
        wait_to_go = EventletEvent()

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

        stack_request = Mock()
        LogIdContainer = get_log_id_container(stack_request)

        runner = ServiceRunner(config=rabbit_config,
                               container_cls=LogIdContainer)
        runner.add_service(EventListeningServiceOne)
        runner.add_service(EventListeningServiceTwo)
        runner.add_service(EventRaisingService)
        wait_to_go.send(runner.start())

        with wait_for_call(5, one_called), wait_for_call(5, two_called):
            runner.stop()

        assert get_id.call_count == 3
        stack_request.assert_has_calls([
            call(None),
            call(['0']),
            call(['0']),
        ])
