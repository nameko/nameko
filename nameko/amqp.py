import amqp
from kombu import Connection
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


def verify_amqp_uri(amqp_uri):
    # From `kombu.transport.pyamqy.Transport.establish_connection`.  We let
    # kombu parse the uri and supply defaults.
    connection = Connection(amqp_uri)
    if connection.transport_cls != 'amqp':
        return
    transport = connection.transport

    conninfo = transport.client
    for name, default_value in transport.default_connection_params.items():
        if not getattr(conninfo, name, None):
            setattr(conninfo, name, default_value)
    if conninfo.hostname == 'localhost':
        conninfo.hostname = '127.0.0.1'
    opts = dict({
        'host': conninfo.host,
        'userid': conninfo.userid,
        'password': conninfo.password,
        'login_method': conninfo.login_method,
        'virtual_host': conninfo.virtual_host,
        'insist': conninfo.insist,
        'ssl': conninfo.ssl,
        'connect_timeout': conninfo.connect_timeout,
        'heartbeat': conninfo.heartbeat,
    }, **conninfo.transport_options or {})
    with ConnectionTester(**opts):
        pass


