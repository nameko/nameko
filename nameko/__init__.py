from __future__ import absolute_import
from logging import getLogger
import sys
import traceback

from nameko import context
from nameko import exceptions
from nameko import sending
from nameko.logging import log_time

_log = getLogger(__name__)


def delegate_apply(delegate, context, method, args):
    try:
        func = getattr(delegate, method)
    except AttributeError:
        raise exceptions.MethodNotFound(method)
    return func(context=context, **args)


def process_message(connection, delegate, body, reraise=False):
    msgid, ctx, method, args = context.parse_message(body)

    _log.debug('processing message `%s`: using %s(...)', msgid, method)

    with log_time(
            _log.debug, 'processed message `%s` in %0.3f sec.', msgid):
        try:
            ret = delegate_apply(delegate, ctx, method, args)
        except Exception:
            exc_typ, exc_val, exc_tb = sys.exc_info()
            if msgid:
                tbfmt = traceback.format_exception(exc_typ, exc_val, exc_tb)
                ret = (exc_typ.__name__, str(exc_val), tbfmt)
                sending.reply(connection, msgid, failure=ret)
            if reraise:
                raise exc_typ, exc_val, exc_tb
        else:
            if msgid:
                _log.debug('replying to message `%s`', msgid)
                sending.reply(connection, msgid, replydata=ret)
