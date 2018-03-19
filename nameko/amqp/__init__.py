# backwards compat imports
from .utils import verify_amqp_uri  # noqa: F401
from .publish import (  # noqa: F401
    get_connection, get_producer, UndeliverableMessage)

from amqp.transport import _AbstractTransport

# backport https://github.com/celery/py-amqp/commit/7db1b2d4a5cfc546ab941697ae7035ff34645a5a#diff-e3b09014ce12269dff5eece1f9f7c082
del _AbstractTransport.__del__
