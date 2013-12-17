""" Service integration testing best practice.
"""
# Nameko relies on eventlet
# You should monkey patch the standard library as early as possible to avoid
# importing anything before the patch is applied.
# See http://eventlet.net/doc/patching.html#monkeypatching-the-standard-library
import eventlet
eventlet.monkey_patch()

from nameko.rpc import rpc, rpc_proxy
from nameko.runners import ServiceRunner
from nameko.testing.utils import get_container
from nameko.testing.service.integration import entrypoint_hook


class ServiceX(object):
    name = "service_x"

    y = rpc_proxy("service_y")

    @rpc
    def remote_method(self, value):
        res = "{}-x".format(value)
        return self.y.append_identifier(res)


class ServiceY(object):
    name = "service_y"

    @rpc
    def append_identifier(self, value):
        return "{}-y".format(value)

#==============================================================================
# Begin test
#==============================================================================

config = {'AMQP_URI': 'amqp://guest:guest@localhost:5672/'}
runner = ServiceRunner(config)
runner.add_service(ServiceX)
runner.add_service(ServiceY)
runner.start()

container = get_container(runner, ServiceX)

with entrypoint_hook(container, "remote_method") as entrypoint:
    assert entrypoint("value") == "value-x-y"

runner.stop()
