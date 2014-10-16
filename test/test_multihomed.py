from functools import partial

from nameko.rpc import rpc
from nameko.runners import ServiceRunner


AMQP_URI_A = "amqp://guest:guest@localhost:5672/a"
AMQP_URI_B = "amqp://guest:guest@localhost:5672/b"

a_rpc = partial(rpc, uri=AMQP_URI_A)
b_rpc = partial(rpc, uri=AMQP_URI_B)


def get_config_value(key, container):
    return container.config.get(key)


class Service(object):

    #external_rpc = rpc_proxy('external', uri=)

    # all equivalent
    @rpc(uri=AMQP_URI_A)
    @rpc(uri=partial(get_config_value, "AMQP_URI_A"))
    @a_rpc
    def method_a(self):
        pass

    # all equivalent
    @rpc(uri=AMQP_URI_B)
    @rpc(uri=partial(get_config_value, "AMQP_URI_B"))
    @b_rpc
    def method_b(self):
        pass

    # without explicit uri we fall back to config['AMQP_URI']
    @rpc
    def method_c(self):
        pass


def test_multihomed(rabbit_config):

    config = {
        'AMQP_URI': rabbit_config['AMQP_URI'],
        'AMQP_URI_A': AMQP_URI_A,
        'AMQP_URI_B': AMQP_URI_B
    }

    runner = ServiceRunner(config)
    runner.add_service(Service)
    runner.start()

    import ipdb; ipdb.set_trace()
    pass
