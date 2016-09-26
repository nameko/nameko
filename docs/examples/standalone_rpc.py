from nameko.standalone.rpc import ClusterRpcProxy

config = {
    'AMQP_URI': AMQP_URI  # e.g. "pyamqp://guest:guest@localhost"
}

with ClusterRpcProxy(config) as cluster_rpc:
    cluster_rpc.service_x.remote_method("hellø")  # "hellø-x-y"
