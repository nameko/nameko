from newrpc import sending

__all__ = ['call', ]

CONTROL_EXCHANGE = 'rpc'


def call(connection, context, topic, msg, timeout=None, options=None):
    if options is not None:
        exchange = options.get('CONTROL_EXCHANGE', CONTROL_EXCHANGE)
    else:
        exchange = CONTROL_EXCHANGE
    return sending.send_rpc(connection,
        context=context,
        exchange=exchange,
        topic=topic,
        method=msg['method'],
        args=msg['args'],
        timeout=timeout)
