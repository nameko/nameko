from nameko.dependencies import dependency_decorator, get_decorator_providers


@dependency_decorator
def consume(foo):
    '''consume-doc'''
    return 'consumer-provider {}'.format(foo)


def test_dependency_decorator():
    # make sure consume is properly wrapped
    assert consume.__doc__ == 'consume-doc'
    assert consume.func_name == 'consume'

    def foo(spam):
        pass

    decorated_foo = consume(foo='bar')(foo)

    # make sure dependency_deocorators pass through the decorated method
    decorated_foo is foo


def test_get_decorator_providers():

    class Foobar(object):
        @consume('spam')
        def shrub(self, arg):
            return arg

    foo = Foobar()
    providers = list(get_decorator_providers(foo))

    assert providers == [('shrub', 'consumer-provider spam')]
