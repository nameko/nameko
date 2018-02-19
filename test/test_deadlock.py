import itertools
import time

import pytest

from nameko.events import event_handler
from nameko.rpc import RpcProxy, rpc
from nameko.standalone.events import event_dispatcher
from nameko.testing.services import entrypoint_waiter


class TestDeadlockRegression(object):
    """ Regression test for https://github.com/nameko/nameko/issues/428
    """

    @pytest.fixture
    def config(self, rabbit_config):
        config = rabbit_config.copy()
        config['max_workers'] = 2
        return config

    @pytest.fixture
    def upstream(self, container_factory, config):

        class Service(object):
            name = "upstream"

            @rpc
            def method(self):
                time.sleep(.5)

        container = container_factory(Service, config)
        container.start()

    @pytest.fixture
    def service_cls(self):

        class Service(object):
            name = "downsteam"

            upstream_rpc = RpcProxy("upstream")

            @event_handler('service', 'event1')
            def handle_event1(self, event_data):
                self.upstream_rpc.method()

            @event_handler('service', 'event2')
            def handle_event2(self, event_data):
                self.upstream_rpc.method()

        return Service

    @pytest.mark.usefixtures('upstream')
    def test_deadlock_due_to_slow_workers(
        self, service_cls, container_factory, config
    ):
        """ Deadlock will occur if the unack'd messages grows beyond the
        size of the worker pool at any point. The QueueConsumer will block
        waiting for a worker and pending RPC replies will not be ack'd.
        Any running workers therefore never complete, and the worker pool
        remains exhausted.
        """
        container = container_factory(service_cls, config)
        container.start()

        count = 2

        dispatch = event_dispatcher(config)
        for _ in range(count):
            dispatch("service", "event1", 1)
            dispatch("service", "event2", 1)

        counter = itertools.count(start=1)

        def cb(worker_ctx, res, exc_info):
            if next(counter) == count:
                return True

        with entrypoint_waiter(
            container, 'handle_event1', timeout=5, callback=cb
        ):
            pass
