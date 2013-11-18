# Nameko relies on eventlet
# You should monkey patch the standard library as early as possible to avoid
# importing anything before the patch is applied.
# See http://eventlet.net/doc/patching.html#monkeypatching-the-standard-library
import eventlet
eventlet.monkey_patch()

import uuid

from kombu import Exchange

from nameko.messaging import publisher
from nameko.runners import ServiceRunner
from nameko.timer import timer


demo_ex = Exchange('demo_ex', durable=False, auto_delete=True)


class MessagingPublisher(object):

    publish = publisher(exchange=demo_ex)

    @timer(interval=2)
    def send_msg(self):
        msg = "log-{}".format(uuid.uuid4())
        self.publish(msg)


def main():
    import logging
    logging.basicConfig(level=logging.DEBUG)

    config = {'AMQP_URI': 'amqp://guest:guest@localhost:5672/'}
    runner = ServiceRunner(config)
    runner.add_service(MessagingPublisher)
    runner.start()

    try:
        runner.wait()
    except KeyboardInterrupt:
        runner.stop()

if __name__ == '__main__':
    main()
