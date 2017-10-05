import bcrypt
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
            user = self.users.get(username)
            if not user:
                raise Unauthenticated("User does not exist")
            if not bcrypt.checkpw(password.encode('utf-8'), user['password']):
                raise Unauthenticated("Incorrect password")

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

            return False

        def check_role(self, role):
            if self.has_role(role):
                return
            raise Unauthorized()

    def setup(self):
        self.db = {
            'matt': {
                'password': (
                    b'$2b$12$fZXR7Z1Eoyn0pfym8.'
                    b'LyRuIFabYj00ZzhdaJ0qoTLZs9w4fg3pKlK'
                ),
                'roles': [
                    'developer',
                ]
            },
            'susie': {
                'password': (
                    b'$2b$12$k4MVi9PcbSsOqONoj5vW9.'
                    b'pcQpB0xSjYkZcc6Ogr5nQ4MD8DRDiUK'
                ),
                'roles': [
                    'developer',
                    'admin'
                ]
            }
        }

    def get_dependency(self, worker_ctx):
        return Auth.Api(self.db, worker_ctx)
