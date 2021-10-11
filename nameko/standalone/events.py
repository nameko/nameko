from kombu import Exchange

from nameko import config, serialization
from nameko.amqp.publish import Publisher
from nameko.constants import (
    AMQP_SSL_CONFIG_KEY, AMQP_URI_CONFIG_KEY, DEFAULT_AMQP_URI, PERSISTENT
)


def get_event_exchange(service_name):
    """ Get an exchange for ``service_name`` events.
    """
    disable_exchange_declaration = not config.get("DECLARE_EVENT_EXCHANGES")

    exchange_name = "{}.events".format(service_name)
    exchange = Exchange(
        exchange_name,
        type='topic',
        durable=True,
        delivery_mode=PERSISTENT,
        no_declare=disable_exchange_declaration,
    )

    return exchange


def event_dispatcher(**kwargs):
    """ Return a function that dispatches nameko events.
    """
    amqp_uri = config.get(AMQP_URI_CONFIG_KEY, DEFAULT_AMQP_URI)
    amqp_uri = kwargs.pop('uri', amqp_uri)

    serializer, _ = serialization.setup()
    serializer = kwargs.pop('serializer', serializer)

    ssl = config.get(AMQP_SSL_CONFIG_KEY)
    ssl = kwargs.pop('ssl', ssl)

    # TODO: standalone event dispatcher should accept context event_data
    # and insert a call id

    publisher = Publisher(amqp_uri, serializer=serializer, ssl=ssl, **kwargs)

    def dispatch(service_name, event_type, event_data):
        """ Dispatch an event claiming to originate from `service_name` with
        the given `event_type` and `event_data`.
        """
        exchange = get_event_exchange(service_name)

        publisher.publish(
            event_data,
            exchange=exchange,
            routing_key=event_type
        )

    return dispatch
