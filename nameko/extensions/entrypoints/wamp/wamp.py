from nameko.extensions import (
    Entrypoint, ProviderCollector, SharedExtension)


class WampRpcConsumer(SharedExtension, ProviderCollector):
	def setup(self):
		# connect here
		pass

	def stop(self):
		# disconnect here


class WAMP(Entrypoint):
	rpc_consumer = WampRpcConsumer()
