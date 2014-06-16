from nameko.events import Event
from nameko.rpc import rpc


class ExampleService(object):
    name = 'nameko_example'

    @rpc
    def method(self):
        pass

    def red_herring(self):
        pass


class ExampleEvent(Event):
    type = 'nameko_example_event'
