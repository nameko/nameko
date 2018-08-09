from __future__ import absolute_import

import amqp
import six
from amqp.exceptions import NotAllowed
from kombu import Connection
from kombu.transport.pyamqp import Transport


BAD_VHOST = (
    'Error connecting to broker, probably caused by using an invalid '
    'or unauthorized vhost'
)


class ConnectionTester(amqp.Connection):
    """Kombu doesn't have any good facilities for diagnosing rabbit
    connection errors, e.g. bad credentials, or unknown vhost. This hack
    attempts some heuristic diagnosis"""

    def __init__(self, *args, **kwargs):
        try:
            super(ConnectionTester, self).__init__(*args, **kwargs)
        except IOError as exc:  # pragma: no cover (rabbitmq >= 3.6.0)
            six.raise_from(IOError(BAD_VHOST), exc)
        except NotAllowed as exc:  # pragma: no cover (rabbitmq < 3.6.0)
            six.raise_from(IOError(BAD_VHOST), exc)


class TestTransport(Transport):
    Connection = ConnectionTester


def verify_amqp_uri(amqp_uri, ssl=None):
    connection = Connection(amqp_uri, ssl=ssl)
    if connection.transport_cls not in ('amqp', 'pyamqp'):
        # Can't use these heuristics. Fall back to the existing error behaviour
        return

    transport = TestTransport(connection.transport.client)
    with transport.establish_connection():
        pass
