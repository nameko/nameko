from nameko.service import ServiceRunner


class TestService1(object):
    pass


class TestService2(object):
    pass


def test_runner_lifecycle():
    events = []

    class Container(object):
        def __init__(self, service_name, service_cls):
            self.service_name = service_name
            self.service_cls = service_cls

        def start(self):
            events.append(('start', self.service_name, self.service_cls))

        def stop(self):
            events.append(('stop', self.service_name, self.service_cls))

        def kill(self):
            events.append(('kill', self.service_name, self.service_cls))

        def wait(self):
            events.append(('wait', self.service_name, self.service_cls))

    runner = ServiceRunner(Container)

    runner.add_service('foobar_1', TestService1)
    runner.add_service('foobar_2', TestService2)

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
    runner.kill()
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
