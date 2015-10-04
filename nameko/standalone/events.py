from kombu import Connection, Exchange
from kombu.pools import producers, connections

from nameko.constants import (
    DEFAULT_RETRY_POLICY, SERIALIZER_CONFIG_KEY,
    DEFAULT_SERIALIZER)

from nameko.messaging import PERSISTENT, AMQP_URI_CONFIG_KEY


def get_event_exchange(service_name):
    """ Get an exchange for ``service_name`` events.
    """
    exchange_name = "{}.events".format(service_name)
    exchange = Exchange(
        exchange_name, type='topic', durable=True, auto_delete=True,
        delivery_mode=PERSISTENT)

    return exchange


def event_dispatcher(nameko_config, **kwargs):
    """ Return a function that dispatches nameko events.
    """

    kwargs = kwargs.copy()
    retry = kwargs.pop('retry', True)
    retry_policy = kwargs.pop('retry_policy', DEFAULT_RETRY_POLICY)

    def dispatch(service_name, event_type, event_data):
        """ Dispatch an event claiming to originate from `service_name` with
        the given `event_type` and `event_data`.
        """
        conn = Connection(nameko_config[AMQP_URI_CONFIG_KEY])

        serializer = nameko_config.get(
            SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER)

        exchange = get_event_exchange(service_name)

        with connections[conn].acquire(block=True) as connection:
            exchange.maybe_bind(connection)
            with producers[conn].acquire(block=True) as producer:
                msg = event_data
                routing_key = event_type
                producer.publish(
                    msg,
                    exchange=exchange,
                    serializer=serializer,
                    routing_key=routing_key,
                    retry=retry,
                    retry_policy=retry_policy,
                    **kwargs)
    return dispatch
