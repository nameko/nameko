import pytest

from nameko.events import event_handler
from nameko.standalone.events import event_dispatcher
from nameko.testing.services import entrypoint_waiter


class ServiceB:
    """ Event listening service.
    """
    name = "service_b"

    @event_handler("service_a", "event_type")
    def handle_event(self, payload):
        print("service b received", payload)


@pytest.mark.usefixtures("rabbit_config")
def test_event_interface(container_factory):

    container = container_factory(ServiceB)
    container.start()

    dispatch = event_dispatcher()

    # prints "service b received payload" before "exited"
    with entrypoint_waiter(container, 'handle_event'):
        dispatch("service_a", "event_type", "payload")
    print("exited")
