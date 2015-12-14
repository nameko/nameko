from nameko.extensions.entrypoints.wamp import wamp


class ExampleService(object):
    name = 'nameko_example'

    @wamp
    def method(self):
        pass



def test_wamp_broker(container_factory, wamp_config):
    container = container_factory(ExampleService, web_config)
    container.start()
