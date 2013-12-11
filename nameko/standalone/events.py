from contextlib import contextmanager

from kombu.common import maybe_declare
from kombu.pools import producers, connections
from kombu import Connection

from nameko.events import get_event_exchange
from nameko.messaging import AMQP_URI_CONFIG_KEY


@contextmanager
def event_dispatcher(container_service_name, nameko_config):
    """ Yield a function that dispatches events claiming to originate from
    a service called `container_service_name`.

    Enables services not hosted by nameko to dispatch events into a nameko
    cluster.
    """
    conn = Connection(nameko_config[AMQP_URI_CONFIG_KEY])
    exchange = get_event_exchange(container_service_name)

    with connections[conn].acquire(block=True) as connection:
        maybe_declare(exchange, connection)

        with producers[conn].acquire(block=True) as producer:

            def dispatch(evt):
                msg = evt.data
                routing_key = evt.type
                producer.publish(msg, exchange=exchange,
                                 routing_key=routing_key)

            yield dispatch
