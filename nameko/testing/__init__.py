
from nameko import context
from nameko import sending


class TestProxy(object):
    def __init__(self, get_connection, timeout=1, service=None, method=None):
        self.get_connection = get_connection
        self.timeout = timeout
        self.service = service
        self.method = method

    def __getattr__(self, key):
        service = self.service

        if service is None:
            service = key
            method = None
        else:
            method = key

        return self.__class__(
            self.get_connection, self.timeout, service, method)

    def __call__(self, **kwargs):
        ctx = context.get_admin_context()
        with self.get_connection() as conn:
            return sending.send_rpc(
                conn, ctx, 'testrpc',
                self.service, self.method, args=kwargs,
                timeout=self.timeout)
