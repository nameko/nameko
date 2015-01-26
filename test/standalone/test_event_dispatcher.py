from mock import Mock

from nameko.events import event_handler
from nameko.standalone.events import event_dispatcher
from nameko.testing.services import entrypoint_waiter

handler_called = Mock()


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

    dispatch = event_dispatcher(config)
    with entrypoint_waiter(container, 'handler', timeout=1):
        dispatch('srcservice', 'testevent', msg)
    handler_called.assert_called_once_with(msg)
