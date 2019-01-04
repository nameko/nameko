from nameko.runners import ServiceRunner
from nameko.testing.utils import get_container

class ServiceA:
    name = "service_a"

class ServiceB:
    name = "service_b"

# create a runner for ServiceA and ServiceB
runner = ServiceRunner()
runner.add_service(ServiceA)
runner.add_service(ServiceB)

# ``get_container`` will return the container for a particular service
container_a = get_container(runner, ServiceA)

# start both services
runner.start()

# stop both services
runner.stop()
