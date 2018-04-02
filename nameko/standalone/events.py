from kombu import Exchange

from nameko.amqp.publish import Publisher
from nameko.constants import (
    AMQP_URI_CONFIG_KEY, DEFAULT_SERIALIZER, PERSISTENT, SERIALIZER_CONFIG_KEY
)


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
    amqp_uri = nameko_config[AMQP_URI_CONFIG_KEY]

    serializer = kwargs.pop(
        'serializer',
        nameko_config.get(
            SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER
        )
    )

    # TODO: standalone event dispatcher should accept context event_data
    # and insert a call id

    publisher = Publisher(amqp_uri, serializer=serializer, **kwargs)

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
