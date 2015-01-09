from kombu.common import maybe_declare
from kombu.pools import producers, connections
from kombu import Connection

from nameko.constants import DEFAULT_RETRY_POLICY
from nameko.events import get_event_exchange, Event
from nameko.messaging import AMQP_URI_CONFIG_KEY


def event_dispatcher(nameko_config, **kwargs):
    """ Returns a function that dispatches events claiming to originate from
    a service called `container_service_name`.

    Enables services not hosted by nameko to dispatch events into a nameko
    cluster.
    """

    kwargs = kwargs.copy()
    retry = kwargs.pop('retry', True)
    retry_policy = kwargs.pop('retry_policy', DEFAULT_RETRY_POLICY)

    def dispatch(service_name, event_type, event_data):
        conn = Connection(nameko_config[AMQP_URI_CONFIG_KEY])

        exchange = get_event_exchange(service_name)
        if isinstance(event_type, type) and issubclass(event_type, Event):
            event_type = event_type.type

        with connections[conn].acquire(block=True) as connection:
            maybe_declare(exchange, connection)
            with producers[conn].acquire(block=True) as producer:
                msg = event_data
                routing_key = event_type
                producer.publish(
                    msg,
                    exchange=exchange,
                    routing_key=routing_key,
                    retry=retry,
                    retry_policy=retry_policy,
                    **kwargs)
    return dispatch
