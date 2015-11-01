import json

from nameko.web.handlers import HttpRequestHandler
from werkzeug.wrappers import Response


class ApplicationError(Exception):
    pass


class HttpError(Exception):

    def __init__(self, payload, status_code=None):
        self.payload = payload
        self.status_code = status_code
        super(HttpError, self).__init__(payload)


class HttpEntrypoint(HttpRequestHandler):

    def response_from_exception(self, exc):
        if isinstance(exc, HttpError):
            response = Response(
                json.dumps(exc.payload),
                status=exc.status_code,
                mimetype='application/json'
            )
            return response
        else:
            return HttpRequestHandler.response_from_exception(self, exc)

http = HttpEntrypoint.decorator


class Service(object):
    name = "http_exceptions_service"

    @http('GET', '/expected_exception', expected_exceptions=ApplicationError)
    def expected_exception(self, request):
        raise ApplicationError("Invalid request")

    @http('GET', '/expected_custom_exception',
          expected_exceptions=HttpError)
    def expected_custom_exception(self, request):
        raise HttpError(json.dumps({
            'error': 'INVALID_REQUEST',
            'description': 'This is invalid request.',
            'code': 400
        }), 400)
