with ClusterRpcProxy(config) as cluster_rpc:
    hello_res = cluster_rpc.service_x.remote_method.call_async("hello")
    world_res = cluster_rpc.service_x.remote_method.call_async("world")
    # do work while waiting
    hello_res.result()  # "hello-x-y"
    world_res.result()  # "world-x-y"
