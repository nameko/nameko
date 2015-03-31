from nameko.containers import ServiceContainer

class Service(object):
    name = "service"

# create a container
container = ServiceContainer(Service, config={})

# ``container.extensions`` exposes all extensions used by the service
service_extensions = list(container.extensions)

# start service
container.start()

# stop service
container.stop()
