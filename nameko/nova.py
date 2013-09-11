from __future__ import absolute_import

from nameko import sending


CONTROL_EXCHANGE = 'rpc'
DEFAULT_RPC_TIMEOUT = 10


def _get_exchange(options):
    if options is not None:
        return options.get('CONTROL_EXCHANGE', CONTROL_EXCHANGE)
    return CONTROL_EXCHANGE


def call(connection, context, topic, msg,
         timeout=DEFAULT_RPC_TIMEOUT, options=None):
    exchange = _get_exchange(options)
    return sending.send_rpc(
        connection,
        context=context,
        exchange=exchange,
        topic=topic,
        method=msg['method'],
        args=msg['args'],
        timeout=timeout)
