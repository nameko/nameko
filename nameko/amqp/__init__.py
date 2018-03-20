# backwards compat imports
from .utils import verify_amqp_uri  # noqa: F401
from .publish import (  # noqa: F401
    get_connection, get_producer, UndeliverableMessage)

from amqp.transport import _AbstractTransport

# backport http://bit.do/do-not-implement-del
del _AbstractTransport.__del__
