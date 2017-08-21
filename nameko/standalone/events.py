import uuid

from kombu import Exchange

from nameko.amqp.publish import Publisher
from nameko.constants import DEFAULT_SERIALIZER, SERIALIZER_CONFIG_KEY
from nameko.messaging import AMQP_URI_CONFIG_KEY, PERSISTENT, HeaderEncoder


CALL_ID_PREFIX = "standalone_event_dispatcher"


def get_event_exchange(service_name):
    """ Get an exchange for ``service_name`` events.
    """
    exchange_name = "{}.events".format(service_name)
    exchange = Exchange(
        exchange_name, type='topic', durable=True, auto_delete=True,
        delivery_mode=PERSISTENT)

    return exchange


def event_dispatcher(
    nameko_config, context_data=None, call_id_prefix=CALL_ID_PREFIX, **options
):
    """ Return a function that dispatches nameko events.
    """
    amqp_uri = nameko_config[AMQP_URI_CONFIG_KEY]

    serializer = options.pop(
        'serializer',
        nameko_config.get(
            SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER
        )
    )

    if context_data is None:
        context_data = {}

    publisher = Publisher(
        amqp_uri, serializer=serializer, **options
    )

    def dispatch(service_name, event_type, event_data):
        """ Dispatch an event claiming to originate from `service_name` with
        the given `event_type` and `event_data`.
        """
        exchange = get_event_exchange(service_name)

        call_id = '{}.{}.{}.{}'.format(
            call_id_prefix, service_name, event_type, uuid.uuid4()
        )
        context_data['call_id_stack'] = [call_id]
        extra_headers = HeaderEncoder().apply_prefix(context_data)

        publisher.publish(
            event_data,
            exchange=exchange,
            routing_key=event_type,
            extra_headers=extra_headers
        )

    return dispatch
