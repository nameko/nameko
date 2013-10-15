# Nameko relies on eventlet
# You should monkey patch the standard library as early as possible to avoid
# importing anything before the patch is applied.
# See http://eventlet.net/doc/patching.html#monkeypatching-the-standard-library
import eventlet
eventlet.monkey_patch()

import os
import tempfile

from kombu import Exchange, Queue

from nameko.dependencies import AttributeDependency
from nameko.messaging import consume
from nameko.service import ServiceRunner

demo_ex = Exchange('demo_ex', durable=False, auto_delete=True)
demo_queue = Queue('demo_queue', exchange=demo_ex, durable=False, auto_delete=True)


class LogFile(AttributeDependency):

    def __init__(self, path=None):
        if path is None:
            path = os.path.join(tempfile.mkdtemp(), 'nameko.log')
        self.path = path

    def start(self, srv_ctx):
        self.file_handle = open(self.path, 'w')

    def stop(self, srv_ctx):
        self.file_handle.close()

    def acquire_injection(self, worker_ctx):
        def log(msg):
            self.file_handle.write(msg + "\n")
        return log

    def release_injection(self, worker_ctx):
        self.file_handle.flush()


class MessagingConsumer(object):

    log = LogFile()

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
