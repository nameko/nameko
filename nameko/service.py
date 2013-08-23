from __future__ import absolute_import

from logging import getLogger

from eventlet.greenpool import GreenPool

from nameko.dependencies import get_dependencies, DependencySet

_log = getLogger(__name__)

WORKER_TIMEOUT = 30


def get_service_name(service_cls):
    return getattr(service_cls, "name", service_cls.__name__.lower())


class ServiceContext(object):
    """ Context for a ServiceContainer
    """
    def __init__(self, name, service_class, container, config=None):
        self.name = name
        self.service_class = service_class
        self.container = container
        self.config = config


class WorkerContext(object):
    """ Context for a Worker
    """
    def __init__(self, srv_ctx, service, method_name, args=None, kwargs=None,
                 data=None):
        self.srv_ctx = srv_ctx
        self.config = srv_ctx.config
        self.service = service
        self.method_name = method_name
        self.args = args if args is not None else ()
        self.kwargs = kwargs if kwargs is not None else {}
        self.data = data if data is not None else {}


class ServiceContainer(object):

    def __init__(self, service_cls, config):
        self.service_cls = service_cls
        self.config = config

        self._ctx = None
        self._dependencies = None
        self._worker_pool = GreenPool(size=self.config.get('poolsize', 100))

    @property
    def dependencies(self):
        if self._dependencies is None:
            self._dependencies = DependencySet()
            # process dependencies: save their name onto themselves
            # TODO: move the name setting into the dependency creation
            for name, dep in get_dependencies(self.service_cls):
                dep.name = name
                self._dependencies.add(dep)
        return self._dependencies

    @property
    def ctx(self):
        # TODO: why ctx a lazy property?
        if self._ctx is None:
            self._ctx = ServiceContext(get_service_name(self.service_cls),
                                       self.service_cls, self, self.config)
        return self._ctx

    def start(self):
        _log.debug('container starting')
        self.dependencies.all.start(self.ctx)
        self.dependencies.all.on_container_started(self.ctx)

    def stop(self):
        _log.debug('container stopping')

        self.dependencies.all.stop(self.ctx)
        self.dependencies.all.on_container_stopped(self.ctx)

    def spawn_worker(self, provider, args, kwargs, context_data=None):
        method_name = provider.name

        _log.debug('spawning worker for method_name: {}'.format(method_name))

        worker_ctx = WorkerContext(
            self.ctx, method_name, args, kwargs, data=context_data)

        def worker():
            service = self.service_cls()
            worker_ctx.service = service

            self.dependencies.all.call_setup(worker_ctx)

            result = exc = None
            method = getattr(service, method_name)
            try:
                result = method(*args, **kwargs)
            except Exception as e:
                exc = e

            # TODO: should we only call the call_result() handler on the
            #       dependency which initiated the worker?
            # self.dependencies.all.call_result(worker_ctx, result, exc)
            provider.call_result(worker_ctx, result, exc)

            self.dependencies.all.call_teardown(worker_ctx)

        self._worker_pool.spawn(worker)

        return worker_ctx
