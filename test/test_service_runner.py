from nameko.service import ServiceRunner


class TestService1(object):
    pass


class TestService2(object):
    pass


def test_runner_lifecycle():
    events = []

    class Container(object):
        def __init__(self, service_cls):
            self.service_cls = service_cls

        def start(self):
            events.append(('start', self.service_cls))

        def stop(self):
            events.append(('stop', self.service_cls))

        def kill(self):
            events.append(('kill', self.service_cls))

        def wait(self):
            events.append(('wait', self.service_cls))

    runner = ServiceRunner(Container)

    runner.add_service(TestService1)
    runner.add_service(TestService2)

    runner.start()

    assert sorted(events) == [
        ('start', TestService1),
        ('start', TestService2),
    ]

    events = []
    runner.stop()
    assert sorted(events) == [
        ('stop', TestService1),
        ('stop', TestService2),
    ]

    events = []
    runner.kill()
    assert sorted(events) == [
        ('kill', TestService1),
        ('kill', TestService2),
    ]

    events = []
    runner.wait()
    assert sorted(events) == [
        ('wait', TestService1),
        ('wait', TestService2),
    ]
