from nameko.constants import (
    LANGUAGE_CONTEXT_KEY, USER_ID_CONTEXT_KEY, AUTH_TOKEN_CONTEXT_KEY)
from nameko.dependencies import InjectionProvider, DependencyFactory, injection


class ContextDataProvider(InjectionProvider):

    def __init__(self, context_key):
        self.context_key = context_key

    def acquire_injection(self, worker_ctx):
        return worker_ctx.data.get(self.context_key)


@injection
def language():
    return DependencyFactory(ContextDataProvider, LANGUAGE_CONTEXT_KEY)


@injection
def user_id():
    return DependencyFactory(ContextDataProvider, USER_ID_CONTEXT_KEY)


@injection
def auth_token():
    return DependencyFactory(ContextDataProvider, AUTH_TOKEN_CONTEXT_KEY)
