from nameko.constants import (
    AUTH_TOKEN_CONTEXT_KEY, LANGUAGE_CONTEXT_KEY, USER_AGENT_CONTEXT_KEY,
    USER_ID_CONTEXT_KEY
)
from nameko.extensions import DependencyProvider


class ContextDataProvider(DependencyProvider):
    context_key = None

    def get_dependency(self, worker_ctx):
        return worker_ctx.data.get(self.context_key)


class Language(ContextDataProvider):
    context_key = LANGUAGE_CONTEXT_KEY


class UserId(ContextDataProvider):
    context_key = USER_ID_CONTEXT_KEY


class UserAgent(ContextDataProvider):
    context_key = USER_AGENT_CONTEXT_KEY


class AuthToken(ContextDataProvider):
    context_key = AUTH_TOKEN_CONTEXT_KEY
