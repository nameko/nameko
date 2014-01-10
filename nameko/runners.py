from __future__ import absolute_import

from contextlib import contextmanager
from logging import getLogger

from nameko.utils import SpawningProxy
from nameko.containers import (
    ServiceContainer, WorkerContext, get_service_name
)


_log = getLogger(__name__)


class ServiceRunner(object):
    """ Allows the user to serve a number of services concurrently.
    The caller can register a number of service classes with a name and
    then use the start method to serve them and the stop and kill methods
    to stop them. The wait method will block until all services have stopped.

    Example::

        runner = ServiceRunner(config)
        runner.add_service(Foobar)
        runner.add_service(Spam, CustomWorkerContext)

        add_sig_term_handler(runner.kill)

        runner.start()

        runner.wait()
    """
    def __init__(self, config, container_cls=ServiceContainer):
        self.service_map = {}
        self.containers = []
        self.config = config
        self.container_cls = container_cls

    def add_service(self, cls, worker_ctx_cls=WorkerContext):
        """ Add a service class to the runner.
        There can only be one service class for a given service name.
        Service classes must be registered before calling start()
        """
        service_name = get_service_name(cls)
        self.service_map[service_name] = (cls, worker_ctx_cls)

    def start(self):
        """ Start all the registered services.

        A new container is created for each service using the container
        class provided in the __init__ method.

        All containers are started concurently and the method will block
        until all have completed their startup routine.
        """
        config = self.config
        service_map = self.service_map
        _log.info('starting services: %s', service_map.keys())

        for _, (service_cls, worker_ctx_cls) in service_map.items():
            container = self.container_cls(service_cls, worker_ctx_cls, config)
            self.containers.append(container)

        SpawningProxy(self.containers).start()

        _log.info('services started: %s', service_map.keys())

    def stop(self):
        """ Stop all running containers concurrently.
        The method blocks until all containers have stopped.
        """
        service_map = self.service_map
        _log.info('stopping services: %s', service_map.keys())

        SpawningProxy(self.containers).stop()

        _log.info('services stopped: %s', service_map.keys())

    def kill(self, exc):
        """ Kill all running containers concurrently.
        The method will block until all containers have stopped.
        """

        service_map = self.service_map
        _log.info('killing services: %s', service_map.keys())

        SpawningProxy(self.containers).kill(exc)

        _log.info('services killed: %s ', service_map.keys())

    def wait(self):
        """ Wait for all running containers to stop.
        """
        SpawningProxy(self.containers).wait()


@contextmanager
def contextual_runner(config, *services, **kwargs):
    """ Serves a number of services for a contextual block.
    The caller can specify a number of service classes then serve them either
    stopping (default) or killing them on exiting the contextual block.


    Example::

        with contextual_runner(config, Foobar, Spam) as runner:
            # interact with services and stop them on exiting the block

        # services stopped


    Additional configuration available to :class:``ServiceRunner`` instances
    can be specified through keyword arguments::

        with contextual_runner(config, Foobar, Spam,
                               container_cls=CustomServiceContainer,
                               worker_ctx_cls=CustomWorkerContext,
                               kill_on_exit=True):
            # interact with services

        # services killed

    :Parameters:
        config : dict
            Configuration to instantiate the service containers with
        services : service definitions
            Services to be served for the contextual block
        container_cls : container class (default = :class:`ServiceContainer`)
            The container class for hosting the specified ``services``
        worker_ctx_cls : worker context class (default = :class:`WorkerContext`)
            The constructor for creating a worker context within a service
            container
        kill_on_exit : bool (default=False)
            If ``True``, run ``kill()`` on the service containers when exiting
            the contextual block. Otherwise ``stop()`` will be called on the
            service containers on exiting the block.

    :Returns: The configured :class:`ServiceRunner` instance

    """

    container_cls = kwargs.pop('container_cls', ServiceContainer)
    worker_ctx_cls = kwargs.pop('worker_ctx_cls', WorkerContext)
    kill_on_exit = kwargs.pop('kill_on_exit', False)

    runner = ServiceRunner(config, container_cls=container_cls)
    for service in services:
        runner.add_service(service, worker_ctx_cls=worker_ctx_cls)

    runner.start()

    yield runner

    if kill_on_exit:
        runner.kill(Exception('killed by contextual service runner'))
    else:
        runner.stop()
