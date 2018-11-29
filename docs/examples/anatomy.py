from nameko.rpc import ServiceRpc, rpc


class Service:
    name = "service"

    # we depend on the RPC interface of "another_service"
    other_rpc = ServiceRpc("another_service")

    @rpc  # `method` is exposed over RPC
    def method(self):
        # application logic goes here
        pass
