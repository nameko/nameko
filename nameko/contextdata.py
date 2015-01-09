from nameko.constants import (
    LANGUAGE_CONTEXT_KEY, USER_ID_CONTEXT_KEY, USER_AGENT_CONTEXT_KEY,
    AUTH_TOKEN_CONTEXT_KEY,
)
from nameko.dependencies import InjectionProvider


class ContextDataProvider(InjectionProvider):
    def acquire_injection(self, worker_ctx):
        return worker_ctx.data.get(self.context_key)


class language(ContextDataProvider):
    context_key = LANGUAGE_CONTEXT_KEY


class user_id(ContextDataProvider):
    context_key = USER_ID_CONTEXT_KEY


class user_agent(ContextDataProvider):
    context_key = USER_AGENT_CONTEXT_KEY


class auth_token(ContextDataProvider):
    context_key = AUTH_TOKEN_CONTEXT_KEY
