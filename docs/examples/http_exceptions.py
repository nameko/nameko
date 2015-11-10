import json
from nameko.web.handlers import HttpRequestHandler
from werkzeug.wrappers import Response


class HttpError(Exception):
    error_code = 'BAD_REQUEST'
    status_code = 400


class InvalidArgumentsError(HttpError):
    error_code = 'INVALID_ARGUMENTS'


class HttpEntrypoint(HttpRequestHandler):
    def response_from_exception(self, exc):
        if isinstance(exc, HttpError):
            response = Response(
                json.dumps({
                    'error': exc.error_code,
                    'message': str(exc),
                }),
                status=exc.status_code,
                mimetype='application/json'
            )
            return response
        return HttpRequestHandler.response_from_exception(self, exc)


http = HttpEntrypoint.decorator


class Service(object):
    name = "http_service"

    @http('GET', '/custom_exception')
    def custom_exception(self, request):
        raise InvalidArgumentsError("Argument `foo` is required.")