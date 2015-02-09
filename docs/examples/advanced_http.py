from nameko.web import http


class Service(object):

    @http('GET', '/404')
    def does_not_exist(self, value):
        return 404, "Does Not Exist"

    @http('GET', '/google')
    def redirect(self,):
        return 302, "", {'Location': 'https://www.google.com/'}
