from nameko.rpc import rpc

from .auth import Auth, Unauthorized


class Service:
    name = "service"

    auth = Auth()

    @rpc(expected_exceptions=Unauthorized)
    def update(self, data):
        if not self.auth.has_role("admin"):
            raise Unauthorized()

        # perform update
        raise TypeError("Whoops, genuine error.")
