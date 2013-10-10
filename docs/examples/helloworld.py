from nameko.service import ServiceRunner
from nameko.events import Event, EventDispatcher, event_handler
from nameko.timer import timer

class HelloEvent(Event):
    type = "hello"

class HelloWorld(object):
  
    @event_handler('friendlyservice', 'hello')
    def hello(self, name):
        print "Hello, {}!".format(name)

class FriendlyService(object):
    
    name = "friendlyservice"
    dispatch = EventDispatcher()
    
    @timer(interval=5)
    def say_hello(self):
        self.dispatch(HelloEvent(self.name))


def main():
	import logging
	logging.basicConfig(level=logging.DEBUG)

	import eventlet
	eventlet.monkey_patch()

	config = {'AMQP_URI': 'amqp://guest:guest@localhost:5672/'}
	runner = ServiceRunner(config)
	runner.add_service(HelloWorld)
	runner.add_service(FriendlyService)
	runner.start()
	try:
	    runner.wait()
	except KeyboardInterrupt:
	    runner.stop()

if __name__ == '__main__':
	main()
