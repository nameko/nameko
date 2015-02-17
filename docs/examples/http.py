# http.py

from nameko.web.handlers import http

class HttpService(object):

    @http('GET', '/get/<int:value>')
    def get_method(self, value):
        return {'value': value}

    @http('POST', '/post')
    def do_post(self, body):
        return "received: {}".format(body)
