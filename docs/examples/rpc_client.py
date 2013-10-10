import random

from nameko.rpc import Service
from nameko.service import ServiceRunner
from nameko.timer import timer


class RpcClient(object):

    adder = Service('adderservice')

    @timer(interval=2)
    def add(self):
        x = random.randint(0, 10)
        y = random.randint(0, 10)
        res = self.adder.add(x, y)
        print "{} + {} = {}".format(x, y, res)


def main():
    import logging
    logging.basicConfig(level=logging.DEBUG)

    import eventlet
    eventlet.monkey_patch()

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
