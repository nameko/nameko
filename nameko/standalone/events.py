from contextlib import contextmanager

from kombu.common import maybe_declare
from kombu.pools import producers, connections
from kombu import Connection

from nameko.constants import DEFAULT_RETRY_POLICY
from nameko.events import get_event_exchange
from nameko.messaging import AMQP_URI_CONFIG_KEY


@contextmanager
def event_dispatcher(container_service_name, nameko_config, **kwargs):
    """ Yield a function that dispatches events claiming to originate from
    a service called `container_service_name`.

    Enables services not hosted by nameko to dispatch events into a nameko
    cluster.
    """
    conn = Connection(nameko_config[AMQP_URI_CONFIG_KEY])
    exchange = get_event_exchange(container_service_name)

    retry = kwargs.pop('retry', True)
    retry_policy = kwargs.pop('retry_policy', DEFAULT_RETRY_POLICY)

    with connections[conn].acquire(block=True) as connection:
        maybe_declare(exchange, connection)

        with producers[conn].acquire(block=True) as producer:

            def dispatch(evt):
                msg = evt.data
                routing_key = evt.type
                producer.publish(
                    msg, exchange=exchange, routing_key=routing_key,
                    retry=retry, retry_policy=retry_policy, **kwargs)

            yield dispatch
