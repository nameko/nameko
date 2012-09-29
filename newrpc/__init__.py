from newrpc import context
from newrpc import sending


def delegate_apply(delegate, body):
    msgid, ctx, method, args = context.parse_message(body)
    try:
        func = getattr(delegate, method)
    except AttributeError:
        raise NotImplementedError()
    try:
        ret = func(context=ctx, **args)
    except Exception:
        raise NotImplementedError()
    return msgid, ret


def delegate_applyreply(connection, delegate, body, reraise=False):
    try:
        msgid, ret = delegate_apply(delegate, body)
    except Exception:
        raise NotImplementedError()
    return sending.reply(connection, msgid, replydata=ret)
