from nameko.standalone.rpc import ClusterRpcClient


config = {
    'AMQP_URI': AMQP_URI  # e.g. "pyamqp://guest:guest@localhost"
}

with ClusterRpcClient(config) as cluster_rpc:
    cluster_rpc.service_x.remote_method("hellø")  # "hellø-x-y"
