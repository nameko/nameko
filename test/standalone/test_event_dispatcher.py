from mock import Mock

from nameko.events import Event, event_handler
from nameko.standalone.events import event_dispatcher
from nameko.testing.utils import wait_for_call

handler_called = Mock()


class TestEvent(Event):
    type = "testevent"


class Service(object):
    name = 'destservice'

    @event_handler('srcservice', 'testevent')
    def handler(self, msg):
        handler_called(msg)


def test_dispatch(container_factory, rabbit_config):
    config = rabbit_config

    container = container_factory(Service, config)
    container.start()

    msg = "msg"

    with event_dispatcher('srcservice', config) as dispatch:
        dispatch(TestEvent(msg))

        with wait_for_call(1, handler_called):
            handler_called.assert_called_once_with(msg)
