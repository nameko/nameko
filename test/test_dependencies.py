from nameko.dependencies import (
    dependency_decorator, get_decorator_providers, DependencyProvider)


class ConsumerProvider(DependencyProvider):
    def __init__(self, args, kwargs):
        self.args = args
        self.kwargs = kwargs


@dependency_decorator
def consume(*args, **kwargs):
    """consume-doc"""
    return ConsumerProvider(args, kwargs)


def test_dependency_decorator():
    # make sure consume is properly wrapped
    assert consume.__doc__ == 'consume-doc'
    assert consume.func_name == 'consume'

    def foo(spam):
        pass

    decorated_foo = consume(foo='bar')(foo)

    # make sure dependency_deocorator passes through the decorated method
    assert decorated_foo is foo


def test_get_decorator_providers():

    class Foobar(object):
        @consume('spam', oof="rab")
        def shrub(self, arg):
            return arg

    foo = Foobar()
    providers = list(get_decorator_providers(foo))

    name, provider = providers.pop()
    assert name == "shrub"
    assert isinstance(provider, ConsumerProvider)
    assert provider.args == ("spam",)
    assert provider.kwargs == {"oof": "rab"}
    assert len(providers) == 0
