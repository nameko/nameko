from nameko.web import http


class Service(object):

    @http('GET', '/get/<int:value>')
    def get_method(self, value):
        return {'value': value}

    @http('POST', '/post')
    def do_post(self, value):
        return {'value': value}
