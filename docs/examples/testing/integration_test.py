""" Service integration testing best practice.
"""

import pytest

from nameko.rpc import ServiceRpc, rpc
from nameko.testing.services import entrypoint_hook
from nameko.testing.utils import get_container


class ServiceX:
    """ Service under test
    """
    name = "service_x"

    y = ServiceRpc("service_y")

    @rpc
    def remote_method(self, value):
        res = "{}-x".format(value)
        return self.y.append_identifier(res)


class ServiceY:
    """ Service under test
    """
    name = "service_y"

    @rpc
    def append_identifier(self, value):
        return "{}-y".format(value)


@pytest.mark.usefixtures("rabbit_config")
def test_service_x_y_integration(runner_factory):

    # run services in the normal manner
    runner = runner_factory(ServiceX, ServiceY)
    runner.start()

    # artificially fire the "remote_method" entrypoint on ServiceX
    # and verify response
    container = get_container(runner, ServiceX)
    with entrypoint_hook(container, "remote_method") as entrypoint:
        assert entrypoint("value") == "value-x-y"
