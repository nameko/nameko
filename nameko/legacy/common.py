import datetime
import uuid

import iso8601


def UIDGEN():
    return uuid.uuid4().hex


def UTCNOW():
    return datetime.datetime.now(iso8601.iso8601.UTC)
