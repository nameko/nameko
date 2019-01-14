from nameko import config
from nameko.standalone.rpc import ClusterRpcClient


config.setup({
    'AMQP_URI': AMQP_URI  # e.g. "pyamqp://guest:guest@localhost"
})

with ClusterRpcClient() as cluster_rpc:
    cluster_rpc.service_x.remote_method("hellø")  # "hellø-x-y"
