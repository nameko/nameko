""" Service unit testing best practice.
"""

from nameko.rpc import rpc_proxy, rpc
from nameko.testing.service.unit import instance_factory


class Service(object):
    math = rpc_proxy("math_service")

    @rpc
    def add(self, x, y):
        return self.math.add(x, y)

    @rpc
    def subtract(self, x, y):
        return self.math.subtract(x, y)

#==============================================================================
# Begin test
#==============================================================================

# create instance
service = instance_factory(Service)

# mock out "math" service
service.math.add.side_effect = lambda x, y: x + y
service.math.subtract.side_effect = lambda x, y: x - y

# test add business logic
assert service.add(3, 4) == 7
service.math.add.assert_called_once_with(3, 4)

# test subtract business logic
assert service.subtract(5, 2) == 3
service.math.subtract.assert_called_once_with(5, 2)
