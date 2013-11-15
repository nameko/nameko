from nameko.legacy import decorators


class _Tester(object):
    __name__ = 'Tester'  # for functools.wraps()

    def __init__(self, failtimes=1, failexc=IOError):
        self.failtimes = failtimes
        self.tries = 0
        self.failexc = failexc

    def __call__(self, channel, *args, **kwargs):
        self.tries += 1
        if self.tries <= self.failtimes:
            raise self.failexc()


def test_autoretry(connection):
    tester = _Tester()
    connection.transport.connection_errors = (tester.failexc, )

    decorators.autoretry(tester)(connection)
    assert tester.tries == 2


def test_ensure(connection):
    tester = _Tester()
    connection.transport.connection_errors = (tester.failexc, )

    decorators.ensure(tester)(connection)
    assert tester.tries == 2
