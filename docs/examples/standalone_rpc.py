from nameko.standalone.rpc import ClusterRpcProxy

config = {
    'AMQP_URI': 'amqp://guest:guest@localhost/nameko'
}

with ClusterRpcProxy(config) as cluster_rpc:
    cluster_rpc.service_x.remote_method("hello")  # "hello-x-y"
