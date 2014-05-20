from mock import Mock
import pytest

from nameko.containers import WorkerContext, PARENT_CALLS_KEY
from nameko.language import LanguageProvider


@pytest.fixture
def mock_container():
    container = Mock()
    container.config = {PARENT_CALLS_KEY: 0}
    return container


def test_get_language(mock_container):

    provider = LanguageProvider()
    provider.bind('language', mock_container)

    worker_ctx = WorkerContext(
        mock_container, "service", provider, data={'language': 'en'})

    assert provider.acquire_injection(worker_ctx) == "en"


def test_get_language_not_set(mock_container):

    provider = LanguageProvider()
    provider.bind('language', mock_container)

    worker_ctx = WorkerContext(
        mock_container, "service", provider, data={})

    assert provider.acquire_injection(worker_ctx) is None
