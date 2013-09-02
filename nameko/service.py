from __future__ import absolute_import

from logging import getLogger

from eventlet.event import Event
from eventlet.greenpool import GreenPool
from nameko.dependencies import get_dependencies, DependencySet

_log = getLogger(__name__)


MAX_WOKERS_KEY = 'max_workers'


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

        if config is not None:
            self.max_workers = config.get(MAX_WOKERS_KEY, 10) or 10
        else:
            self.max_workers = 10


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

    def __str__(self):
        return '<WorkerContext {}.{} at 0x{:x}>'.format(
            self.srv_ctx.name, self.method_name, id(self))


class ServiceContainer(object):

    def __init__(self, service_cls, config):
        self.service_cls = service_cls
        self.config = config
        self.ctx = ServiceContext(get_service_name(self.service_cls),
                                  self.service_cls, self, self.config)
        self._dependencies = None
        self._worker_pool = GreenPool(size=self.ctx.max_workers)
        self._died = Event()

    # TODO: why is this a lazy property?
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

    def start(self):
        _log.debug('starting %s', self)
        self.dependencies.all.start(self.ctx)
        self.dependencies.all.on_container_started(self.ctx)

    def stop(self):
        _log.debug('stopping %s', self)
        dependencies = self.dependencies

        # decorator deps have to be stopped before attribute deps
        # to ensure that running workers can successfully complete
        dependencies.decorators.all.stop(self.ctx)

        # there might still be some running workers, which we have to
        # wait for to complete before we can stop attribute dependencies
        self._worker_pool.waitall()
        dependencies.attributes.all.stop(self.ctx)

        dependencies.all.on_container_stopped(self.ctx)
        self._died.send(None)

    def spawn_worker(self, provider, args, kwargs,
                     context_data=None, handle_result=None):

        service = self.service_cls()
        worker_ctx = WorkerContext(
            self.ctx, service, provider.name, args, kwargs, data=context_data)

        _log.debug('spawning %s', worker_ctx)
        self._worker_pool.spawn(self._run_worker, worker_ctx, handle_result)

        # TODO: should we link with the new thread to handle/re-raise errors?

        return worker_ctx

    def _run_worker(self, worker_ctx, handle_result):
        _log.debug('setting up %s', worker_ctx)
        self.dependencies.all.call_setup(worker_ctx)

        result = exc = None
        try:
            _log.debug('calling handler for %s', worker_ctx)
            method = getattr(worker_ctx.service, worker_ctx.method_name)
            result = method(*worker_ctx.args, **worker_ctx.kwargs)
        except Exception as e:
            exc = e

        if handle_result is not None:
            _log.debug('handling result for %s', worker_ctx)
            handle_result(worker_ctx, result, exc)

        _log.debug('signalling result for %s', worker_ctx)
        self.dependencies.attributes.all.call_result(
            worker_ctx, result, exc)

        _log.debug('tearing down %s', worker_ctx)
        self.dependencies.all.call_teardown(worker_ctx)

    def wait(self):
        return self._died.wait()

    def __str__(self):
        return '<ServiceContainer {} at 0x{:x}>'.format(
            self.ctx.name, id(self))
