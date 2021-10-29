import itertools
import time

import pytest
from kombu.messaging import Queue
from mock import Mock

from nameko import config
from nameko.amqp.publish import Publisher
from nameko.constants import DEFAULT_HEARTBEAT, HEARTBEAT_CONFIG_KEY
from nameko.events import EventHandler, event_handler
from nameko.messaging import Consumer, consume
from nameko.rpc import ReplyListener, RpcConsumer, ServiceRpc, rpc
from nameko.standalone.events import event_dispatcher
from nameko.standalone.rpc import ServiceRpcClient
from nameko.testing.services import entrypoint_waiter, once
from nameko.testing.utils import get_extension


class TestDeadlockRegression(object):
    """ Regression test for https://github.com/nameko/nameko/issues/428
    """

    @pytest.yield_fixture
    def config(self, rabbit_config):
        with config.patch({'max_workers': 2}):
            yield

    @pytest.fixture
    def upstream(self, container_factory, config):

        class Service(object):
            name = "upstream"

            @rpc
            def method(self):
                time.sleep(.5)

        container = container_factory(Service)
        container.start()

    @pytest.fixture
    def service_cls(self):

        class Service(object):
            name = "downsteam"

            upstream_rpc = ServiceRpc("upstream")

            @event_handler('service', 'event1')
            def handle_event1(self, event_data):
                self.upstream_rpc.method()

            @event_handler('service', 'event2')
            def handle_event2(self, event_data):
                self.upstream_rpc.method()

        return Service

    @pytest.mark.usefixtures('config')
    @pytest.mark.usefixtures('upstream')
    def test_deadlock_due_to_slow_workers(
        self, service_cls, container_factory
    ):
        """
        Implementation has now changed, but keeping this test as a regression.

        Deadlock would occur if the unack'd messages grows beyond the
        size of the worker pool at any point. The QueueConsumer would block
        waiting for a worker and pending RPC replies would not be ack'd.
        Any running workers therefore never complete, and the worker pool
        remains exhausted.
        """
        container = container_factory(service_cls)
        container.start()

        count = 2

        dispatch = event_dispatcher()
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


class TestHeartbeats(object):

    @pytest.fixture
    def service_cls(self):
        class Service(object):
            name = "service"

            upstream_rpc = ServiceRpc("upstream")

            @consume(queue=Queue(name="queue"))
            @event_handler("service", "event")
            @rpc
            def echo(self, arg):
                return arg  # pragma: no cover

        return Service

    @pytest.mark.usefixtures('rabbit_config')
    @pytest.mark.parametrize(
        'extension_cls', [Consumer, EventHandler, RpcConsumer, ReplyListener]
    )
    def test_default(self, extension_cls, service_cls, container_factory):

        container = container_factory(service_cls)
        container.start()

        extension = get_extension(container, extension_cls)
        assert extension.consumer.heartbeat == DEFAULT_HEARTBEAT

    @pytest.mark.usefixtures('rabbit_config')
    @pytest.mark.parametrize("heartbeat", [30, None])
    @pytest.mark.parametrize(
        'extension_cls', [Consumer, EventHandler, RpcConsumer, ReplyListener]
    )
    def test_config_value(
        self, extension_cls, heartbeat, service_cls, container_factory
    ):
        with config.patch({HEARTBEAT_CONFIG_KEY: heartbeat}):
            container = container_factory(service_cls)
            container.start()

            extension = get_extension(container, extension_cls)
            assert extension.consumer.heartbeat == heartbeat


class TestHeartbeatFailure(object):

    @pytest.fixture
    def config(self, rabbit_config):
        with config.patch({
            'max_workers': 2,
            HEARTBEAT_CONFIG_KEY: 3  # minimum reliable heartbeat
        }):
            yield

    @pytest.fixture
    def tracker(self):
        return Mock()

    @pytest.mark.usefixtures('config')
    def test_event_handler(
        self, container_factory, tracker, queue_info
    ):

        class Service(object):
            name = "service"

            @once
            def work(self):
                # consume a worker
                time.sleep(999)

            @event_handler('service', 'event')
            def handle(self, event_data):
                time.sleep(9)  # >2x heartbeat
                tracker(event_data)

        container = container_factory(Service)
        container.start()

        # wait for service to handle two requests
        counter = itertools.count(start=1)
        with entrypoint_waiter(
            container, 'handle', callback=lambda *a: next(counter) == 2
        ):
            # dispatch two messages; the second will block on the worker pool
            dispatch = event_dispatcher()
            dispatch("service", "event", 1)
            dispatch("service", "event", 2)

        # expect entrypoint to only have run twice and consumed all messages.
        # more than twice or remaining messages means the heartbeat failed
        # and a message was requeued or redelivered
        assert tracker.call_count == 2
        assert queue_info(
            "evt-service-event--service.handle"
        ).message_count == 0

    @pytest.mark.usefixtures('config')
    def test_consumer(self, container_factory, tracker, queue_info):

        queue = Queue(name="queue")

        class Service(object):
            name = "service"

            @once
            def work(self):
                # consume a worker
                time.sleep(999)

            @consume(queue)
            def handle(self, payload):
                time.sleep(9)  # >3x heartbeat
                tracker(payload)

        container = container_factory(Service)
        container.start()

        # wait for service to handle two requests
        counter = itertools.count(start=1)
        with entrypoint_waiter(
            container, 'handle', callback=lambda *a: next(counter) == 2
        ):
            # publish two messages; the second will block on the worker pool
            publisher = Publisher(
                config['AMQP_URI'], routing_key=queue.name
            )
            publisher.publish(1)
            publisher.publish(2)

        # expect entrypoint to only have run twice and consumed all messages.
        # more than twice or remaining messages means the heartbeat failed
        # and a message was requeued or redelivered
        assert tracker.call_count == 2
        assert queue_info(queue.name).message_count == 0

    @pytest.mark.usefixtures('config')
    def test_rpc(self, container_factory, tracker, queue_info):

        class Service(object):
            name = "service"

            @once
            def work(self):
                # consume a worker
                time.sleep(999)

            @rpc
            def method(self, arg):
                time.sleep(9)  # >2x heartbeat
                tracker(arg)

        container = container_factory(Service)
        container.start()

        # wait for service to handle two requests
        counter = itertools.count(start=1)
        with entrypoint_waiter(
            container, 'method', callback=lambda *a: next(counter) == 2
        ):
            # make two requests; the second will block on the worker pool
            with ServiceRpcClient('service') as service_rpc:
                service_rpc.method.call_async(1)
                service_rpc.method.call_async(2)

        # expect entrypoint to only have run twice and consumed all messages.
        # more than twice or remaining messages means the heartbeat failed
        # and a message was requeued or redelivered
        assert tracker.call_count == 2
        assert queue_info("rpc-service").message_count == 0
