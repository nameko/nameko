from mock import Mock

from nameko.service import WorkerContext, ServiceContext


class MockWorkerContext(WorkerContext):

    def __init__(self, srv_ctx=None, service="mockservice",
                 method_name="mockmethod", data_keys=None, **kwargs):
        if srv_ctx is None:
            srv_ctx = Mock(spec=ServiceContext)
        if data_keys is not None:
            self.data_keys = data_keys

        super(MockWorkerContext, self).__init__(
            srv_ctx, service, method_name, **kwargs)
