"""
Utilities for testing nameko services.
"""

from collections import OrderedDict
from contextlib import contextmanager
import inspect

import eventlet
from eventlet import event
from eventlet.semaphore import Semaphore
from mock import MagicMock

from nameko.extensions import Dependency, Entrypoint
from nameko.exceptions import ExtensionNotFound
from nameko.testing.utils import get_extension, wait_for_worker_idle


@contextmanager
def entrypoint_hook(container, method_name, context_data=None):
    """ Yield a function providing an entrypoint into a hosted service.

    The yielded function may be called as if it were the bare method defined
    in the service class. Intended to be used as an integration testing
    utility.

    **Usage**

    To verify that ServiceX and ServiceY are compatible, make an integration
    test that checks their interaction:

    .. literalinclude:: examples/testing/integration_test.py

    """
    entrypoint = get_extension(container, Entrypoint, method_name=method_name)
    if entrypoint is None:
        raise ExtensionNotFound(
            "No entrypoint for '{}' found on container {}.".format(
                method_name, container))

    def hook(*args, **kwargs):
        result = event.Event()

        def handle_result(worker_ctx, res=None, exc_info=None):
            result.send(res, exc_info)
            return res, exc_info

        container.spawn_worker(entrypoint, args, kwargs,
                               context_data=context_data,
                               handle_result=handle_result)

        # If the container errors (e.g. due to a bad entrypoint), handle_result
        # is never called and we hang. To mitigate, we spawn a greenlet waiting
        # for the container, and if that throws we send the exception back
        # as our result
        def catch_container_errors(gt):
            try:
                gt.wait()
            except Exception as exc:
                result.send_exception(exc)

        gt = eventlet.spawn(container.wait)
        gt.link(catch_container_errors)

        return result.wait()

    yield hook


class EntrypointWaiter(Dependency):
    """Helper for `entrypoint_waiter`

    Injection to be manually (and temporarily) added to an existing container.
    Takes an entrypoint name, and exposes a `wait` method, which will return
    once the entrypoint has fired.
    """

    def __init__(self, entrypoint):
        self.attr_name = '_entrypoint_waiter_{}'.format(entrypoint)
        self.entrypoint = entrypoint
        self.done = Semaphore(value=0)

    def worker_teardown(self, worker_ctx):
        entrypoint = worker_ctx.entrypoint
        if entrypoint.method_name == self.entrypoint:
            self.done.release()

    def wait(self):
        self.done.acquire()


@contextmanager
def entrypoint_waiter(container, entrypoint, timeout=30):
    """Helper to wait for entrypoints to fire (and complete)

    Usage::

        container = ServiceContainer(ExampleService, config)
        with entrypoint_waiter(container, 'example_handler'):
            ...  # e.g. rpc call that will result in handler being called
    """

    waiter = EntrypointWaiter(entrypoint)
    if not get_extension(container, Entrypoint, method_name=entrypoint):
        raise RuntimeError("{} has no entrypoint `{}`".format(
            container.service_name, entrypoint))
    if get_extension(container, EntrypointWaiter, entrypoint=entrypoint):
        raise RuntimeError("Waiter already registered for {}".format(
            entrypoint))

    # can't mess with dependencies while container is running
    wait_for_worker_idle(container)
    container.dependencies.add(waiter)

    try:
        yield
        with eventlet.Timeout(timeout):
            waiter.wait()
    finally:
        wait_for_worker_idle(container)
        container.dependencies.remove(waiter)


def worker_factory(service_cls, **injections):
    """ Return an instance of ``service_cls`` with its injected dependencies
    replaced with Mock objects, or as given in ``injections``.

    **Usage**

    The following example service proxies calls to a "math" service via
    and ``RpcProxy`` dependency::

        from nameko.rpc import RpcProxy, rpc

        class ConversionService(object):
            math = RpcProxy("math_service")

            @rpc
            def inches_to_cm(self, inches):
                return self.math.multiply(inches, 2.54)

            @rpc
            def cm_to_inches(self, cms):
                return self.math.divide(cms, 2.54)

    Use the ``worker_factory`` to create an unhosted instance of
    ``ConversionService`` with its injections replaced by Mock objects::

        service = worker_factory(ConversionService)

    Nameko's entrypoints do not modify the service methods, so they can be
    called directly on an unhosted instance. The injection Mocks can be used
    as any other Mock object, so a complete unit test for Service may look
    like this::

        # create worker instance
        service = worker_factory(Service)

        # replace "math" service
        service.math.multiply.side_effect = lambda x, y: x * y
        service.math.divide.side_effect = lambda x, y: x / y

        # test inches_to_cm business logic
        assert service.inches_to_cm(300) == 762
        service.math.multiply.assert_called_once_with(300, 2.54)

        # test cms_to_inches business logic
        assert service.cms_to_inches(762) == 300
        service.math.divide.assert_called_once_with(762, 2.54)

    *Providing Injections*

    The ``**injections`` kwargs to ``worker_factory`` can be used to provide
    a replacement injection instead of a Mock. For example, to unit test a
    service against a real database:

    .. literalinclude:: examples/testing/unit_with_provided_injection_test.py

    If a given injection does not exist on ``service_cls``, a
    ``ExtensionNotFound`` exception is raised.

    """
    service = service_cls()
    for name, attr in inspect.getmembers(service_cls):
        if isinstance(attr, Dependency):
            try:
                injection = injections.pop(name)
            except KeyError:
                injection = MagicMock()
            setattr(service, name, injection)

    if injections:
        raise ExtensionNotFound("Injection(s) '{}' not found on {}.".format(
            injections.keys(), service_cls))

    return service


class MockInjection(Dependency):
    def __init__(self, name):
        self.attr_name = name
        self.injection = MagicMock()

    def acquire_injection(self, worker_ctx):
        return self.injection


def replace_injections(container, *injections):
    """ Replace the injections on ``container`` with :class:`MockInjection`
    objects if they are named in ``injections``.

    Return the :attr:`MockInjection.injection` of the replacements, so that
    calls to the replaced injections can be inspected. Return a single object
    if only one injection was replaced, and a generator yielding the
    replacements in the same order as ``names`` otherwise.

    Replacements are made on the container instance and have no effect on the
    service class. New container instances are therefore unaffected by
    replacements on previous instances.

    **Usage**

    ::

        from nameko.rpc import RpcProxy, rpc
        from nameko.standalone.rpc import ServiceRpcProxy

        class ConversionService(object):
            math = RpcProxy("math_service")

            @rpc
            def inches_to_cm(self, inches):
                return self.math.multiply(inches, 2.54)

            @rpc
            def cm_to_inches(self, cms):
                return self.math.divide(cms, 2.54)

        container = ServiceContainer(ConversionService, config)
        math = replace_injections(container, "math")

        container.start()

        with ServiceRpcProxy('conversionservice', config) as proxy:
            proxy.cm_to_inches(100)

        # assert that the injection was called as expected
        math.divide.assert_called_once_with(100, 2.54)

    """
    if container.started:
        raise RuntimeError('You must replace injections before the '
                           'container is started.')

    dependency_names = {dep.attr_name for dep in container.dependencies}

    missing = set(injections) - dependency_names
    if missing:
        raise ExtensionNotFound("Dependency(s) '{}' not found on {}.".format(
            missing, container))

    replacements = OrderedDict()

    named_dependencies = {dep.attr_name: dep for dep in container.dependencies
                          if dep.attr_name in injections}
    for name in injections:
        dependency = named_dependencies[name]
        replacement = MockInjection(name)
        replacements[dependency] = replacement
        container.dependencies.remove(dependency)
        container.dependencies.add(replacement)

    # if only one name was provided, return any replacement directly
    # otherwise return a generator
    res = (replacement.injection for replacement in replacements.values())
    if len(injections) == 1:
        return next(res)
    return res


def restrict_entrypoints(container, *entrypoints):
    """ Restrict the entrypoints on ``container`` to those named in
    ``entrypoints``.

    This method must be called before the container is started.

    **Usage**

    The following service definition has two entrypoints for "method"::

        class Service(object):

            @rpc
            def foo(self, arg):
                pass

            @rpc
            @event_handler('srcservice', 'event_one')
            def bar(self, arg)
                pass

        container = container_factory(Service, config)

    To disable the entrypoints other than on "foo"::

        restrict_entrypoints(container, "foo")

    To maintain both the rpc and the event_handler entrypoints on "bar"::

        restrict_entrypoints(container, "bar")

    Note that it is not possible to identify entrypoints individually.
    """
    if container.started:
        raise RuntimeError('You must restrict entrypoints before the '
                           'container is started.')

    entrypoint_deps = list(container.entrypoints)
    entrypoint_names = {ext.method_name for ext in entrypoint_deps}

    missing = set(entrypoints) - entrypoint_names
    if missing:
        raise ExtensionNotFound("Entrypoint(s) '{}' not found on {}.".format(
            missing, container))

    for entrypoint in entrypoint_deps:
        if entrypoint.method_name not in entrypoints:
            container.entrypoints.remove(entrypoint)


class Once(Entrypoint):
    """ Entrypoint that spawns a worker exactly once, as soon as
    the service container started.
    """
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def start(self):
        self.container.spawn_worker(self, self.args, self.kwargs)

once = Once.decorator

# dummy entrypoint
dummy = Entrypoint.decorator
