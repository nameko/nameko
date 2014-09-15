from mock import Mock

from nameko.events import Event, event_handler
from nameko.standalone.events import event_dispatcher
from nameko.testing.services import entrypoint_waiter

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
        with entrypoint_waiter(container, 'handler', timeout=1):
            dispatch(TestEvent(msg))
    handler_called.assert_called_once_with(msg)
