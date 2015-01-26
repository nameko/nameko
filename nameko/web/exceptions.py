from nameko.exceptions import BadRequest


class ConnectionNotFound(BadRequest):
    pass


class BadPayload(BadRequest):
    pass
