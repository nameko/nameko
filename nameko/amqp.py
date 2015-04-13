from __future__ import absolute_import

import amqp
from kombu import Connection
from kombu.transport.pyamqp import Transport
import six


class ConnectionTester(amqp.Connection):
    """Kombu doesn't have any good facilities for diagnosing rabbit
    connection errors, e.g. bad credentials, or unknown vhost. This hack
    attempts some heuristic diagnosis"""

    def __init__(self, *args, **kwargs):
        try:
            super(ConnectionTester, self).__init__(*args, **kwargs)
        except IOError as exc:
            if not hasattr(self, '_wait_tune_ok'):
                raise
            elif self._wait_tune_ok:
                six.raise_from(IOError(
                    'Error connecting to broker, probably caused by invalid'
                    ' credentials'
                ), exc)
            else:
                six.raise_from(IOError(
                    'Error connecting to broker, probably caused by using an'
                    ' invalid or unauthorized vhost'
                ), exc)


class TestTransport(Transport):
    Connection = ConnectionTester


def verify_amqp_uri(amqp_uri):
    connection = Connection(amqp_uri)
    if connection.transport_cls != 'amqp':
        # Can't use these heuristics. Fall back to the existing error behaviour
        return

    transport = TestTransport(connection.transport.client)
    with transport.establish_connection():
        pass
