from nameko.rpc import rpc, RpcProxy

class ServiceY(object):
    name = "service_y"

    @rpc
    def append_identifier(self, value):
        return "{}-y".format(value)


class ServiceX(object):
    name = "service_x"

    y = RpcProxy("service_y")

    @rpc
    def remote_method(self, value):
        res = "{}-x".format(value)
        return self.y.append_identifier(res)
