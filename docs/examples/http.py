# http.py

import json
from nameko.web.handlers import http

class HttpService(object):
    name = "http_service"

    @http('GET', '/get/<int:value>')
    def get_method(self, request, value):
        return json.dumps({'value': value})

    @http('POST', '/post')
    def do_post(self, request):
        return "received: {}".format(request.get_data(as_text=True))
