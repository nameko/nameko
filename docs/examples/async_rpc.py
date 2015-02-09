with ClusterRpcProxy(config) as cluster_rpc:
    hello_res = cluster_rpc.service_x.remote_method.async("hello")
    world_res = cluster_rpc.service_x.remote_method.async("world")
    # do work while waiting
    hello_res.wait()  # "x-y-hello"
    world_res.wait()  # "x-y-world"
