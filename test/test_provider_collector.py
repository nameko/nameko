# coding: utf-8

import pytest

import nameko.concurrency
from nameko.extensions import ProviderCollector


def test_provider_collector():
    collector = ProviderCollector()

    provider1 = object()
    provider2 = object()
    collector.register_provider(provider1)
    collector.register_provider(provider2)

    assert provider1 in collector._providers
    assert provider2 in collector._providers

    collector.unregister_provider(provider1)
    assert provider1 not in collector._providers

    # unregister missing provider is a no-op
    collector.unregister_provider(provider1)

    # stop() should block while a provider remains
    with pytest.raises(nameko.concurrency.Timeout):
        with nameko.concurrency.Timeout(0):
            collector.stop()

    # stop() will return when the final provider is unregistered
    with nameko.concurrency.Timeout(0):
        collector.unregister_provider(provider2)
        collector.stop()
