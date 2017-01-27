import warnings
from kombu import Exchange
from six.moves import queue

from nameko.amqp import get_connection, get_producer, UndeliverableMessage
from nameko.constants import (
    DEFAULT_RETRY_POLICY, DEFAULT_SERIALIZER, SERIALIZER_CONFIG_KEY)
from nameko.messaging import AMQP_URI_CONFIG_KEY, PERSISTENT


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

    kwargs = kwargs.copy()
    retry = kwargs.pop('retry', True)
    retry_policy = kwargs.pop('retry_policy', DEFAULT_RETRY_POLICY)
    use_confirms = kwargs.pop('use_confirms', True)
    mandatory = kwargs.pop('mandatory', False)

    def dispatch(service_name, event_type, event_data):
        """ Dispatch an event claiming to originate from `service_name` with
        the given `event_type` and `event_data`.
        """
        serializer = nameko_config.get(
            SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER)

        exchange = get_event_exchange(service_name)

        with get_connection(amqp_uri) as connection:
            exchange.maybe_bind(connection)  # TODO: reqd? maybe_declare?
            with get_producer(amqp_uri, use_confirms) as producer:
                msg = event_data
                routing_key = event_type
                producer.publish(
                    msg,
                    exchange=exchange,
                    serializer=serializer,
                    routing_key=routing_key,
                    retry=retry,
                    retry_policy=retry_policy,
                    mandatory=mandatory,
                    **kwargs)

                if mandatory:
                    if not use_confirms:
                        warnings.warn(
                            "Mandatory delivery was requested, but "
                            "unroutable messages cannot be detected without "
                            "publish confirms enabled."
                        )

                    try:
                        returned_messages = producer.channel.returned_messages
                        returned = returned_messages.get_nowait()
                    except queue.Empty:
                        pass
                    else:
                        raise UndeliverableMessage(returned)

    return dispatch
