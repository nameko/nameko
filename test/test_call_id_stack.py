from logging import getLogger

from eventlet import Timeout
from mock import Mock, patch
from eventlet.event import Event

from nameko.rpc import rpc, rpc_proxy
from nameko.runners import ServiceRunner
from nameko.once import once


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


def test_positive_tracking(reset_rabbit, rabbit_config):
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

        runner = ServiceRunner(config=rabbit_config)
        runner.add_service(Child)
        runner.add_service(Parent)
        runner.add_service(GrandparentDo)
        wait_to_go.send(runner.start())

        with Timeout(30):
            e.wait()
        runner.stop()

        child_do_called.assert_called_with()
        assert child_do_called.call_count == 1

        assert get_id.call_count == 3

