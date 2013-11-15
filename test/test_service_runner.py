import pytest

from nameko.runners import ServiceRunner


class TestService1(object):
    name = 'foobar_1'


class TestService2(object):
    name = 'foobar_2'


def test_runner_lifecycle():
    events = []

    class Container(object):
        def __init__(self, service_cls, worker_ctx_cls, config):
            self.service_name = service_cls.__name__
            self.service_cls = service_cls
            self.worker_ctx_cls = worker_ctx_cls

        def start(self):
            events.append(('start', self.service_cls.name, self.service_cls))

        def stop(self):
            events.append(('stop', self.service_cls.name, self.service_cls))

        def kill(self, exc):
            events.append(('kill', self.service_cls.name, self.service_cls))

        def wait(self):
            events.append(('wait', self.service_cls.name, self.service_cls))

    config = {}
    runner = ServiceRunner(config, container_cls=Container)

    runner.add_service(TestService1)
    runner.add_service(TestService2)

    runner.start()

    assert sorted(events) == [
        ('start', 'foobar_1', TestService1),
        ('start', 'foobar_2', TestService2),
    ]

    events = []
    runner.stop()
    assert sorted(events) == [
        ('stop', 'foobar_1', TestService1),
        ('stop', 'foobar_2', TestService2),
    ]

    events = []
    runner.kill(Exception('die'))
    assert sorted(events) == [
        ('kill', 'foobar_1', TestService1),
        ('kill', 'foobar_2', TestService2),
    ]

    events = []
    runner.wait()
    assert sorted(events) == [
        ('wait', 'foobar_1', TestService1),
        ('wait', 'foobar_2', TestService2),
    ]


def test_runner_waits_raises_error():
    class Container(object):
        def __init__(self, service_cls, worker_ctx_cls, config):
            pass

        def start(self):
            pass

        def stop(self):
            pass

        def kill(self, exc):
            pass

        def wait(self):
            raise Exception('error in container')

    runner = ServiceRunner(config={}, container_cls=Container)
    runner.add_service(TestService1)
    runner.start()

    with pytest.raises(Exception) as exc_info:
        runner.wait()
    assert exc_info.value.args == ('error in container',)
