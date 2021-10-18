from kombu import Exchange

from nameko import config, serialization
from nameko.amqp.publish import Publisher
from nameko.constants import (
    AMQP_SSL_CONFIG_KEY, AMQP_URI_CONFIG_KEY, DEFAULT_AMQP_URI,
    LOGIN_METHOD_CONFIG_KEY, PERSISTENT
)


def get_event_exchange(service_name):
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


def event_dispatcher(**kwargs):
    """ Return a function that dispatches nameko events.
    """
    amqp_uri = config.get(AMQP_URI_CONFIG_KEY, DEFAULT_AMQP_URI)
    amqp_uri = kwargs.pop('uri', amqp_uri)

    serializer, _ = serialization.setup()
    serializer = kwargs.pop('serializer', serializer)

    ssl = config.get(AMQP_SSL_CONFIG_KEY)
    ssl = kwargs.pop('ssl', ssl)

    login_method = config.get(LOGIN_METHOD_CONFIG_KEY)
    login_method = kwargs.pop('login_method', login_method)

    # TODO: standalone event dispatcher should accept context event_data
    # and insert a call id

    publisher = Publisher(
        amqp_uri, serializer=serializer, ssl=ssl, login_method=login_method, **kwargs
    )

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
