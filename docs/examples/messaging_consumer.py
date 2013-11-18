# Nameko relies on eventlet
# You should monkey patch the standard library as early as possible to avoid
# importing anything before the patch is applied.
# See http://eventlet.net/doc/patching.html#monkeypatching-the-standard-library
import eventlet
eventlet.monkey_patch()

import os
import tempfile

from kombu import Exchange, Queue

from nameko.dependencies import InjectionProvider, injection, DependencyFactory
from nameko.messaging import consume
from nameko.runners import ServiceRunner

demo_ex = Exchange('demo_ex', durable=False, auto_delete=True)
demo_queue = Queue('demo_queue', exchange=demo_ex, durable=False,
                   auto_delete=True)


class InvalidPath(Exception):
    pass


class LogFile(InjectionProvider):
    """ Developer docs for LogFile InjectionProvider
    """
    def __init__(self, path):
        if path is None:
            path = os.path.join(tempfile.mkdtemp(), 'nameko.log')
        self.path = path

    def prepare(self):
        self.file_handle = open(self.path, 'w')

    def stop(self):
        self.file_handle.close()

    def acquire_injection(self, worker_ctx):
        def log(msg):
            self.file_handle.write(msg + "\n")
        return log

    def worker_teardown(self, worker_ctx):
        self.file_handle.flush()


@injection
def file_logger(path=None):
    """ User docs for file logger
    """
    if path is not None:
        check_path = path
        if not os.path.exists(check_path):
            check_path = os.path.dirname(path)
        if not os.access(check_path, os.W_OK):
            raise InvalidPath("File or dir not writable: {}".format(path))

    return DependencyFactory(LogFile, path)


class MessagingConsumer(object):

    log = file_logger('/tmp/nameko.log')

    @consume(demo_queue)
    def process(self, payload):
        self.log(payload)


def main():
    import logging
    logging.basicConfig(level=logging.DEBUG)

    config = {'AMQP_URI': 'amqp://guest:guest@localhost:5672/'}
    runner = ServiceRunner(config)
    runner.add_service(MessagingConsumer)
    runner.start()

    try:
        runner.wait()
    except KeyboardInterrupt:
        runner.stop()

if __name__ == '__main__':
    main()
