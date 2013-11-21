from nameko.rpc import standalone_proxy, rpc


class FooService(object):
    name = 'foobar'

    @rpc
    def spam(self, ham):
        return ham


def test_proxy(container_factory, rabbit_config):
    config = rabbit_config

    container = container_factory(FooService, config)
    container.start()
    with standalone_proxy('foobar', config) as foo:
        assert foo.spam(ham='eggs') == 'eggs'
