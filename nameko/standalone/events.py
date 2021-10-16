from kombu import Exchange

from nameko import serialization
from nameko.amqp.publish import Publisher
from nameko.constants import (
    AMQP_SSL_CONFIG_KEY, AMQP_URI_CONFIG_KEY, LOGIN_METHOD_CONFIG_KEY,
    PERSISTENT
)


def get_event_exchange(service_name, config):
    """ Get an exchange for ``service_name`` events.
    """
    auto_delete = config.get("AUTO_DELETE_EVENT_EXCHANGES")
    disable_exchange_declaration = config.get("DECLARE_EVENT_EXCHANGES") is False

    exchange_name = "{}.events".format(service_name)
    exchange = Exchange(
        exchange_name,
        type='topic',
        durable=True,
        delivery_mode=PERSISTENT,
        auto_delete=auto_delete,
        no_declare=disable_exchange_declaration,
    )

    return exchange


def event_dispatcher(nameko_config, **kwargs):
    """ Return a function that dispatches nameko events.
    """
    amqp_uri = nameko_config[AMQP_URI_CONFIG_KEY]

    serializer, _ = serialization.setup(nameko_config)
    serializer = kwargs.pop('serializer', serializer)

    ssl = nameko_config.get(AMQP_SSL_CONFIG_KEY)
    login_method = nameko_config.get(LOGIN_METHOD_CONFIG_KEY)

    # TODO: standalone event dispatcher should accept context event_data
    # and insert a call id

    publisher = Publisher(
        amqp_uri, serializer=serializer, ssl=ssl, login_method=login_method, **kwargs
    )

    def dispatch(service_name, event_type, event_data):
        """ Dispatch an event claiming to originate from `service_name` with
        the given `event_type` and `event_data`.
        """
        exchange = get_event_exchange(service_name, nameko_config)

        publisher.publish(
            event_data,
            exchange=exchange,
            routing_key=event_type
        )

    return dispatch
