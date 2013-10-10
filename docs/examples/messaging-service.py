from kombu import Exchange, Queue

from nameko.dependencies import AttributeDependency
from nameko.messaging import consume, Publisher
from nameko.service import ServiceRunner

demo_ex = Exchange('demo_ex', durable=False, auto_delete=True)
demo_queue = Queue('demo_queue', exchange=demo_ex, durable=False, auto_delete=True)


class LogFile(AttributeDependency):

	def __init__(self, path):
		self.path = path

	def start(self, srv_ctx):
		self.file_handle = open(self.path, 'w')

	def stop(self, srv_ctx):
		self.file_handle.close()

	def acquire_injection(self, worker_ctx):
		def log(msg):
			self.file_handle.write(msg + "\n")
		return log

	def release_injection(self, worker_ctx):
		self.file_handle.flush()



class ListenerService(object):
	
	log = LogFile('/tmp/nameko')

	@consume(demo_queue)
	def process(self, payload):
		self.log(payload)


def main():
	import logging
	logging.basicConfig(level=logging.DEBUG)

	import eventlet
	eventlet.monkey_patch()

	config = {'AMQP_URI': 'amqp://guest:guest@localhost:5672/'}
	runner = ServiceRunner(config)
	runner.add_service(ListenerService)
	runner.start()

	try:
	    runner.wait()
	except KeyboardInterrupt:
	    runner.stop()

if __name__ == '__main__':
	main()