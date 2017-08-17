import jwt

from nameko.extensions import DependencyProvider


JWT_SECRET = "secret"


class Unauthenticated(Exception):
    pass


class Unauthorized(Exception):
    pass


class Auth(DependencyProvider):

    class Api:
        def __init__(self, users, worker_ctx):
            self.users = users
            self.worker_ctx = worker_ctx

        def authenticate(self, username, password):
            try:
                assert self.users[username]['password'] == password
            except (KeyError, AssertionError):
                raise Unauthenticated()

            payload = {
                'username': username,
                'roles': self.users[username]['roles']
            }
            token = jwt.encode(payload, key=JWT_SECRET).decode('utf-8')
            self.worker_ctx.context_data['auth'] = token
            return token

        def has_role(self, role):
            token = self.worker_ctx.context_data.get('auth')
            if not token:
                raise Unauthenticated()

            try:
                payload = jwt.decode(token, key=JWT_SECRET, verify=True)
                if role in payload['roles']:
                    return True
            except Exception:
                pass

            raise Unauthorized()

    def setup(self):
        self.db = {
            'matt': {
                'password': 'secret',
                'roles': [
                    'developer',
                ]
            },
            'susie': {
                'password': 'supersecret',
                'roles': [
                    'developer',
                    'admin'
                ]
            }
        }

    def get_dependency(self, worker_ctx):
        return Auth.Api(self.db, worker_ctx)
