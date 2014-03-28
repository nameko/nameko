""" Service unit testing best practice.
"""

from nameko.rpc import rpc_proxy, rpc
from nameko.testing.services import worker_factory


class ConversionService(object):
    math = rpc_proxy("math_service")

    @rpc
    def inches_to_cm(self, inches):
        return self.math.multiply(inches, 2.54)

    @rpc
    def cms_to_inches(self, cms):
        return self.math.divide(cms, 2.54)

# =============================================================================
# Begin test
# =============================================================================


def test_conversion_service():
    # create instance
    # dependencies are replaced with mocks unless otherwise given - see
    # :class:`nameko.testing.services.worker_factory`
    service = worker_factory(ConversionService)

    # replace "math" service
    service.math.multiply.side_effect = lambda x, y: x * y
    service.math.divide.side_effect = lambda x, y: x / y

    # test inches_to_cm business logic
    assert service.inches_to_cm(300) == 762
    service.math.multiply.assert_called_once_with(300, 2.54)

    # test cms_to_inches business logic
    assert service.cms_to_inches(762) == 300
    service.math.divide.assert_called_once_with(762, 2.54)
