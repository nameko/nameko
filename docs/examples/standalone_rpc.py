from nameko import config
from nameko.standalone.rpc import ClusterRpcProxy


conf = {
    'AMQP_URI': AMQP_URI  # e.g. "pyamqp://guest:guest@localhost"
}
config.setup(conf)

with ClusterRpcProxy(conf) as cluster_rpc:
    cluster_rpc.service_x.remote_method("hellø")  # "hellø-x-y"
