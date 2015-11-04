import json

from nameko.web.handlers import HttpRequestHandler
from werkzeug.wrappers import Response


class ApplicationError(Exception):
    pass


class HttpError(Exception):

    def __init__(self, message, error, status_code):
        self.payload = {
            'error': error,
            'message': message
        }
        self.status_code = status_code
        super(HttpError, self).__init__(self.payload)


class BadRequestError(HttpError):

    def __init__(self, message):
        super(BadRequestError, self).__init__(message, 'BAD_REQUEST', 400)


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
          expected_exceptions=BadRequestError)
    def expected_custom_exception(self, request):
        raise BadRequestError("Argument foo is required.")
