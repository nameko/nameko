import pytest

from nameko.containers import ServiceContainer, WorkerContext
from nameko.constants import (
    AUTH_TOKEN_CONTEXT_KEY, LANGUAGE_CONTEXT_KEY, USER_ID_CONTEXT_KEY)
from nameko.contextdata import (
    language, user_id, auth_token, ContextDataProvider)
from nameko.dependencies import DependencyFactory, injection
from nameko.testing.utils import get_dependency

CUSTOM_CONTEXT_KEY = "custom"


@injection
def custom_value():
    return DependencyFactory(ContextDataProvider, CUSTOM_CONTEXT_KEY)


class CustomWorkerContext(WorkerContext):
    context_keys = WorkerContext.context_keys + (CUSTOM_CONTEXT_KEY,)


class Service(object):

    # builtin context data providers
    auth_token = auth_token()
    language = language()
    user_id = user_id()

    # custom context data provider
    custom_value = custom_value()


@pytest.fixture
def container():
    return ServiceContainer(Service, CustomWorkerContext, {})


def test_get_custom_context_value(container):
    provider = get_dependency(
        container, ContextDataProvider, name="custom_value")
    worker_ctx = WorkerContext(
        container, "service", provider, data={CUSTOM_CONTEXT_KEY: "hello"})

    assert provider.acquire_injection(worker_ctx) == "hello"


def test_get_unset_value(container):
    provider = get_dependency(
        container, ContextDataProvider, name="custom_value")
    worker_ctx = WorkerContext(
        container, "service", provider, data={})

    assert provider.acquire_injection(worker_ctx) is None


@pytest.mark.parametrize('provider_name, context_key', [
    ('auth_token', AUTH_TOKEN_CONTEXT_KEY),
    ('language', LANGUAGE_CONTEXT_KEY),
    ('user_id', USER_ID_CONTEXT_KEY),

])
def test_get_builtin_providers(provider_name, context_key, container):
    provider = get_dependency(
        container, ContextDataProvider, name=provider_name)
    worker_ctx = WorkerContext(
        container, "service", provider, data={context_key: 'value'})

    assert provider.acquire_injection(worker_ctx) == "value"
