""" Service integration testing best practice.
"""

from nameko.rpc import rpc, RpcProxy
from nameko.testing.utils import get_container
from nameko.testing.services import entrypoint_hook


class ServiceX(object):
    """ Service under test
    """
    name = "service_x"

    y = RpcProxy("service_y")

    @rpc
    def remote_method(self, value):
        res = "{}-x".format(value)
        return self.y.append_identifier(res)


class ServiceY(object):
    """ Service under test
    """
    name = "service_y"

    @rpc
    def append_identifier(self, value):
        return "{}-y".format(value)


def test_service_x_y_integration(runner_factory, rabbit_config):

    # run services in the normal manner
    runner = runner_factory(rabbit_config, ServiceX, ServiceY)
    runner.start()

    # artificially fire the "remote_method" entrypoint on ServiceX
    # and verify response
    container = get_container(runner, ServiceX)
    with entrypoint_hook(container, "remote_method") as entrypoint:
        assert entrypoint("value") == "value-x-y"
