from nameko.standalone.rpc import ClusterRpcClient


with ClusterRpcClient(
    uri=AMQP_URI  # e.g. "pyamqp://guest:guest@localhost"
) as cluster_rpc:
    cluster_rpc.service_x.remote_method("hellø")  # "hellø-x-y"
