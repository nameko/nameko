# advanced_http.py

from nameko.web.handlers import http
from werkzeug.wrappers import Response

class Service(object):
    name = "advanced_http_service"

    @http('GET', '/privileged')
    def forbidden(self, request):
        return 403, "Forbidden"

    @http('GET', '/headers')
    def redirect(self, request):
        return 201, {'Location': 'https://www.example.com/widget/1'}, ""

    @http('GET', '/custom')
    def custom(self, request):
        return Response("payload")
