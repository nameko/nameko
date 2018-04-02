from nameko.rpc import rpc, RpcProxy

class ServiceY:
    name = "service_y"

    @rpc
    def append_identifier(self, value):
        return u"{}-y".format(value)


class ServiceX:
    name = "service_x"

    y = RpcProxy("service_y")

    @rpc
    def remote_method(self, value):
        res = u"{}-x".format(value)
        return self.y.append_identifier(res)
