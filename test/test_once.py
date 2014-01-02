from mock import Mock
from nameko.containers import ServiceContainer
from nameko.testing.once import OnceProvider
from nameko.testing.utils import wait_for_call


def test_provider():
    container = Mock(spec=ServiceContainer)
    container.service_name = "service"
    container.config = Mock()

    once = OnceProvider()
    once.bind('foobar', container)
    once.prepare()
    once.start()

    with wait_for_call(1, container.spawn_worker) as spawn_worker:
        once.stop()

    # the once should have stopped and should only have spawned
    # a single worker
    spawn_worker.assert_called_once_with(once, (), {})
