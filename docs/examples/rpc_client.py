# Nameko relies on eventlet
# You should monkey patch the standard library as early as possible to avoid
# importing anything before the patch is applied.
# See http://eventlet.net/doc/patching.html#monkeypatching-the-standard-library
import eventlet
eventlet.monkey_patch()

import logging
logger = logging.getLogger(__name__)

import random

from nameko.rpc import rpc_proxy
from nameko.runners import ServiceRunner
from nameko.timer import timer


class RpcClient(object):

    adder = rpc_proxy('adderservice')

    @timer(interval=2)
    def add(self):
        x = random.randint(0, 10)
        y = random.randint(0, 10)
        res = self.adder.add(x, y)
        logger.info("{} + {} = {}".format(x, y, res))


def main():

    logging.basicConfig(level=logging.DEBUG)

    config = {'AMQP_URI': 'amqp://guest:guest@localhost:5672/'}
    runner = ServiceRunner(config)
    runner.add_service(RpcClient)
    runner.start()

    try:
        runner.wait()
    except KeyboardInterrupt:
        runner.stop()

if __name__ == '__main__':
    main()
