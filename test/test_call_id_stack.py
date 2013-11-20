from logging import getLogger

from eventlet import Timeout
from mock import Mock, patch, call
from eventlet.event import Event
from nameko.containers import ServiceContainer, WorkerContext

from nameko.rpc import rpc, rpc_proxy
from nameko.runners import ServiceRunner
from nameko.once import once, OnceProvider
from nameko.testing.utils import wait_for_call


_log = getLogger(__name__)


child_do_called = Mock()


class Child(object):
    name = 'child'

    @rpc
    def child_do(self):
        child_do_called()
        return 1

    @rpc
    def child_fail(self):
        raise Exception()


class Parent(object):
    child_service = rpc_proxy('child')

    @rpc
    def parent_do(self):
        r = self.child_service.child_do()
        return r

    @rpc
    def parent_fail(self):
        return self.child_service.child_fail()


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


def test_service_container_gets_stack(rabbit_config):
    with patch('nameko.containers.new_call_id') as get_id:
        i = Increment()
        get_id.side_effect = i.next

        stack_request = Mock()
        LogIdContainer = get_log_id_container(stack_request)

        entry = OnceProvider()
        container = LogIdContainer(Child, WorkerContext, rabbit_config)
        entry.bind('foobar', container)
        entry.prepare()
        entry.start()

        with wait_for_call(1, stack_request):
            entry.stop()

        stack_request.assert_called_once_with(None)
        assert get_id.call_count == 1


def test_call_id_stack(reset_rabbit, rabbit_config):
    # Consistent message IDs
    with patch('nameko.containers.new_call_id') as get_id:
        i = Increment()
        get_id.side_effect = i.next
        wait_to_go = Event()
        e = Event()

        class GrandparentDo(object):
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
        runner.add_service(GrandparentDo)
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
            call(['0', '1']),
        ])
