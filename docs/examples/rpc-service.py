from nameko.rpc import rpc
from nameko.service import ServiceRunner


class AdderService(object):
	
	@rpc
	def add(self, x, y):
		return x + y


def main():
	import logging
	logging.basicConfig(level=logging.DEBUG)

	import eventlet
	eventlet.monkey_patch()

	config = {'AMQP_URI': 'amqp://guest:guest@localhost:5672/'}
	runner = ServiceRunner(config)
	runner.add_service(AdderService)
	runner.start()

	try:
	    runner.wait()
	except KeyboardInterrupt:
	    runner.stop()

if __name__ == '__main__':
	main()