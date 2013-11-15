# Nameko relies on eventlet
# You should monkey patch the standard library as early as possible to avoid
# importing anything before the patch is applied.
# See http://eventlet.net/doc/patching.html#monkeypatching-the-standard-library
import eventlet
eventlet.monkey_patch()

import logging
logger = logging.getLogger(__name__)

from nameko.runners import ServiceRunner
from nameko.events import Event, event_dispatcher, event_handler
from nameko.timer import timer


class HelloEvent(Event):
    type = "hello"


class HelloWorld(object):

    @event_handler('friendlyservice', 'hello')
    def hello(self, name):
        logger.info("Hello, {}!".format(name))


class FriendlyService(object):

    name = "friendlyservice"
    dispatch = event_dispatcher()

    @timer(interval=5)
    def say_hello(self):
        self.dispatch(HelloEvent(self.name))


def main():

    logging.basicConfig(level=logging.DEBUG)

    config = {'AMQP_URI': 'amqp://guest:guest@localhost:5672/'}
    runner = ServiceRunner(config)
    runner.add_service(HelloWorld)
    runner.add_service(FriendlyService)
    runner.start()
    try:
        runner.wait()
    except KeyboardInterrupt:
        runner.stop()

if __name__ == '__main__':
    main()
