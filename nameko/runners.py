from __future__ import absolute_import

import warnings
from contextlib import contextmanager
from logging import getLogger

from nameko.containers import get_container_cls, get_service_name
from nameko.utils import SpawningProxy

_log = getLogger(__name__)


class ServiceRunner(object):
    """ Allows the user to serve a number of services concurrently.
    The caller can register a number of service classes with a name and
    then use the start method to serve them and the stop and kill methods
    to stop them. The wait method will block until all services have stopped.

    Example::

        runner = ServiceRunner(config)
        runner.add_service(Foobar)
        runner.add_service(Spam)

        add_sig_term_handler(runner.kill)

        runner.start()

        runner.wait()
    """
    def __init__(self, config, container_cls=None):
        self.service_map = {}
        self.config = config

        if container_cls is not None:
            warnings.warn(
                "The constructor of `ServiceRunner` has changed. "
                "The `container_cls` kwarg is now deprecated. You can "
                "use a custom class by setting the `SERVICE_CONTAINER_CLS` "
                "config option to dotted a class path. This warning will be "
                "removed in version 2.6.0.", DeprecationWarning
            )
        else:
            container_cls = get_container_cls(config)

        self.container_cls = container_cls

    @property
    def service_names(self):
        return self.service_map.keys()

    @property
    def containers(self):
        return self.service_map.values()

    def add_service(self, cls, worker_ctx_cls=None):
        """ Add a service class to the runner.
        There can only be one service class for a given service name.
        Service classes must be registered before calling start()
        """
        if worker_ctx_cls is not None:
            warnings.warn(
                "The signature of `add_service` has changed. "
                "The `worker_ctx_cls` kwarg is now deprecated. See CHANGES, "
                "version 2.4.0 for more details. THis wanring will be removed "
                "in version 2.6.0.", DeprecationWarning
            )

        service_name = get_service_name(cls)
        container = self.container_cls(cls, self.config, worker_ctx_cls)
        self.service_map[service_name] = container

    def start(self):
        """ Start all the registered services.

        A new container is created for each service using the container
        class provided in the __init__ method.

        All containers are started concurrently and the method will block
        until all have completed their startup routine.
        """
        service_names = ', '.join(self.service_names)
        _log.info('starting services: %s', service_names)

        SpawningProxy(self.containers).start()

        _log.debug('services started: %s', service_names)

    def stop(self):
        """ Stop all running containers concurrently.
        The method blocks until all containers have stopped.
        """
        service_names = ', '.join(self.service_names)
        _log.info('stopping services: %s', service_names)

        SpawningProxy(self.containers).stop()

        _log.debug('services stopped: %s', service_names)

    def kill(self):
        """ Kill all running containers concurrently.
        The method will block until all containers have stopped.
        """
        service_names = ', '.join(self.service_names)
        _log.info('killing services: %s', service_names)

        SpawningProxy(self.containers).kill()

        _log.debug('services killed: %s ', service_names)

    def wait(self):
        """ Wait for all running containers to stop.
        """
        try:
            SpawningProxy(self.containers, abort_on_error=True).wait()
        except Exception:
            # If a single container failed, stop its peers and re-raise the
            # exception
            self.stop()
            raise


@contextmanager
def run_services(config, *services, **kwargs):
    """ Serves a number of services for a contextual block.
    The caller can specify a number of service classes then serve them either
    stopping (default) or killing them on exiting the contextual block.


    Example::

        with run_services(config, Foobar, Spam) as runner:
            # interact with services and stop them on exiting the block

        # services stopped


    Additional configuration available to :class:``ServiceRunner`` instances
    can be specified through keyword arguments::

        with run_services(config, Foobar, Spam,
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
        container_cls : container class (default=:class:`ServiceContainer`)
            The container class for hosting the specified ``services``
        worker_ctx_cls : worker context class (default=:class:`WorkerContext`)
            The constructor for creating a worker context within a service
            container
        kill_on_exit : bool (default=False)
            If ``True``, run ``kill()`` on the service containers when exiting
            the contextual block. Otherwise ``stop()`` will be called on the
            service containers on exiting the block.

    :Returns: The configured :class:`ServiceRunner` instance

    """
    kill_on_exit = kwargs.pop('kill_on_exit', False)

    for deprecated_keyword in ('container_cls', 'worker_ctx_cls'):
        if deprecated_keyword in kwargs:
            warnings.warn(
                "The signature of `run_services` has changed. "
                "The `worker_ctx_cls` and `container_cls` kwargs are now "
                "deprecated. You can use a custom `ServiceContainer` class by "
                "setting the `SERVICE_CONTAINER_CLS` config option to a "
                "dotted class path. See CHANGES, version 2.4.0 for more "
                "details. This warning will be removed in version 2.6.0.",
                DeprecationWarning
            )

    container_cls = kwargs.pop('container_cls', None)
    worker_ctx_cls = kwargs.pop('worker_ctx_cls', None)

    runner = ServiceRunner(config, container_cls=container_cls)
    for service in services:
        runner.add_service(service, worker_ctx_cls=worker_ctx_cls)

    runner.start()

    yield runner

    if kill_on_exit:
        runner.kill()
    else:
        runner.stop()
