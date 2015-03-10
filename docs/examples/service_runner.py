from nameko.runners import ServiceRunner
from nameko.utils import get_container

class ServiceA(object):
    pass

class ServiceB(object):
    pass

# create a runner for ServiceA and ServiceB
runner = ServiceRunner(config={})
runner.add_service(ServiceA)
runner.add_service(ServiceB)

# ``get_container`` will return the container for a particular service
container_a = get_container(runner, ServiceA)

# start both services
runner.start()

# stop both services
runner.stop()
