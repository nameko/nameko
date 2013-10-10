import uuid

from kombu import Exchange

from nameko.messaging import Publisher
from nameko.service import ServiceRunner
from nameko.timer import timer


demo_ex = Exchange('demo_ex', durable=False, auto_delete=True)


class MessagingClient(object):

    publish = Publisher(exchange=demo_ex)

    @timer(interval=2)
    def send_msg(self):
        msg = "log-{}".format(uuid.uuid4())
        self.publish(msg)


def main():
    import logging
    logging.basicConfig(level=logging.DEBUG)

    import eventlet
    eventlet.monkey_patch()

    config = {'AMQP_URI': 'amqp://guest:guest@localhost:5672/'}
    runner = ServiceRunner(config)
    runner.add_service(MessagingClient)
    runner.start()

    try:
        runner.wait()
    except KeyboardInterrupt:
        runner.stop()

if __name__ == '__main__':
    main()
