# http_exceptions.py

import json

from nameko.web.handlers import http
from nameko.exceptions import HttpError


class ApplicationError(Exception):
    pass


class ApplicationCustomError(HttpError):
    pass


class Service(object):
    name = "http_exceptions_service"

    @http('GET', '/expected_exception', expected_exceptions=ApplicationError)
    def expected_exception(self, request):
        raise ApplicationError("Invalid request")

    @http('GET', '/expected_custom_exception',
          expected_exceptions=ApplicationCustomError)
    def expected_custom_exception(self, request):
        raise ApplicationCustomError(json.dumps({
            'error': 'INVALID_REQUEST',
            'description': 'This is invalid request.',
            'code': 400
        }), 400)
