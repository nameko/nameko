from nameko.rpc import rpc

from .auth import Auth, Unauthenticated


class Service:
    name = "service"

    auth = Auth()

    @rpc(sensitive_arguments="password", expected_exceptions=Unauthenticated)
    def login(self, username, password):
        # raises Unauthenticated if username/password do not match
        return self.auth.authenticate(username, password)
