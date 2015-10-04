from nameko.standalone.rpc import ClusterRpcProxy

config = {
    'AMQP_URI': AMQP_URI  # e.g. "amqp://guest:guest@localhost"
}

with ClusterRpcProxy(config) as cluster_rpc:
    cluster_rpc.service_x.remote_method("hello")  # "hello-x-y"
