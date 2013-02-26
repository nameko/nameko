import datetime
import uuid

import iso8601

UIDGEN = lambda: uuid.uuid4().hex
UTCNOW = lambda: datetime.datetime.now(iso8601.iso8601.UTC)
