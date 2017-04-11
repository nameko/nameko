""" Regression tests for https://github.com/nameko/nameko/issues/428
"""
import itertools
import time
from datetime import datetime
from functools import partial

import pytest
from mock import Mock, call

from nameko.amqp import get_producer
from nameko.events import event_handler
from nameko.rpc import RpcProxy, rpc
from nameko.standalone.events import event_dispatcher
from nameko.standalone.rpc import ServiceRpcProxy
from nameko.testing.services import entrypoint_hook, entrypoint_waiter
from nameko.web.handlers import http


class TestDeadlock(object):

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
            def method(self, delay):
                time.sleep(delay)

        container = container_factory(Service, config)
        container.start()

    @pytest.fixture
    def service_cls(self):

        class Service(object):
            name = "downsteam"

            upstream_rpc = RpcProxy("upstream")

            @event_handler('service', 'event1')
            def handle_event1(self, event_data):
                self.upstream_rpc.method(event_data)

            @event_handler('service', 'event2')
            def handle_event2(self, event_data):
                self.upstream_rpc.method(event_data)

        return Service

    @pytest.fixture
    def setup_queues(self, service_cls, container_factory, config):
        container = container_factory(service_cls, config)
        container.start()
        time.sleep(.1)
        container.stop()

    @pytest.mark.usefixtures('setup_queues', 'upstream')
    def test_deadlock(
        self, service_cls, container_factory, config
    ):
        """ If the unack'd messages becomes greater than max workers,
        the QueueConsumer will block for the worker pool, and fail to process
        message acks.

        RPC proxies block until they have ack'd their replies, so if running
        workers also make RPC requests, the service deadlocks.

        Since the `prefetch_count` is applied to the channel, which is shared,
        you reach the failure state when the messages are spread across
        multiple entrypoints.
        """
        count = 2

        dispatch = event_dispatcher(config)
        for _ in range(count):
            dispatch("service", "event1", 0)
            dispatch("service", "event2", 0)

        container = container_factory(service_cls, config)
        container.start()

        counters = {
            1: itertools.count(start=1),
            2: itertools.count(start=1)
        }

        def cb(ident, worker_ctx, res, exc_info):
            if next(counters[ident]) == count:
                return True

        with entrypoint_waiter(
            container, 'handle_event1', timeout=1, callback=partial(cb, 1)
        ):
            with entrypoint_waiter(
                container, 'handle_event2', timeout=1, callback=partial(cb, 2)
            ):
                pass

    @pytest.mark.usefixtures('setup_queues', 'upstream')
    def test_deadlock_single_queue(
        self, service_cls, container_factory, config
    ):
        """ Since Nameko==2.5.3 it's possible to reach the failure state with a
        single AMQP entrypoint, since `prefetch_count` is set to
        `max_workers + 1`

        This test passes on Nameko<=2.5.2 but fails on Nameko==2.5.3.
        """
        count = 4

        dispatch = event_dispatcher(config)
        for _ in range(count):
            dispatch("service", "event1", 0)

        container = container_factory(service_cls, config)
        container.start()

        counter = itertools.count(start=1)

        def cb(worker_ctx, res, exc_info):
            if next(counter) == count:
                return True

        with entrypoint_waiter(
            container, 'handle_event1', timeout=1, callback=cb
        ):
            pass

    @pytest.mark.usefixtures('upstream')
    def test_deadlock_single_queue_slow_workers(
        self, service_cls, container_factory, config
    ):
        """ Deadlock will occur if the unack'd messages grows beyond the
        size of the worker pool at any point, for example due to slow workers.

        Only a single entrypoint is used here, so this test passes on
        Nameko<=2.5.2 but fails on Nameko==2.5.3.
        """
        container = container_factory(service_cls, config)
        container.start()

        count = 4

        dispatch = event_dispatcher(config)
        for _ in range(count):
            dispatch("service", "event1", 1)

        counter = itertools.count(start=1)

        def cb(worker_ctx, res, exc_info):
            if next(counter) == count:
                return True

        with entrypoint_waiter(
            container, 'handle_event1', timeout=5, callback=cb
        ):
            pass


class TestLostConsumers(object):

    @pytest.fixture
    def tracker(self):
        return Mock()

    @pytest.fixture
    def config(self, rabbit_config):
        config = {}
        config.update(rabbit_config)
        config['max_workers'] = 2
        config['HEARTBEAT'] = 3
        return config

    @pytest.fixture
    def service_cls(self, tracker):

        class Service(object):
            name = "downsteam"

            @event_handler('service', 'event')
            def handle_event(self, event_data):
                tracker(event_data)
                time.sleep(10)
                return event_data

        return Service

    def test_duplicated_workers(
        self, service_cls, container_factory, config, tracker, web_session
    ):
        """ Blocking on the worker pool longer than 2xHEARTBEAT will cause
        the broker to close the QueueConsumer's connection, and un-ack'd
        messages will be re-queued.

        This test loops forever consuming messsages, blocking the worker pool,
        losing it's connection, and then re-establishing it and re-consuming
        the same messages.
        """

        container = container_factory(service_cls, config)
        container.start()

        results = []

        def cb(worker_ctx, res, exc_info):
            results.append(res)
            return set(results) == set(range(1, 5))

        with entrypoint_waiter(container, 'handle_event', callback=cb):

            dispatch = event_dispatcher(config)
            dispatch("service", "event", 1)
            dispatch("service", "event", 2)
            dispatch("service", "event", 3)
            dispatch("service", "event", 4)

        assert tracker.call_count == 4
        for idx in range(1, 5):
            assert call(idx) in tracker.call_args_list


class TestLostReplies(object):

    @pytest.fixture
    def tracker(self):
        return Mock()

    @pytest.fixture
    def config(self, rabbit_config, web_config):
        config = {}
        config.update(rabbit_config)
        config.update(web_config)
        config['max_workers'] = 2
        config['HEARTBEAT'] = 3
        return config

    @pytest.fixture
    def upstream(self, container_factory, config):

        class Service(object):
            name = "upstream"

            @rpc
            def sleep(self, duration):
                time.sleep(duration)

        container = container_factory(Service, config)
        container.start()

    @pytest.fixture
    def service_cls(self, tracker):

        class Service(object):
            name = "downsteam"

            upstream_rpc = RpcProxy("upstream")

            @rpc
            def method(self, duration):
                self.upstream_rpc.sleep(duration)
                return "OK"

            @rpc
            def sleep(self, duration):
                time.sleep(duration)

        return Service

    @pytest.mark.usefixtures('upstream')
    def test_lost_replies(
        self, service_cls, container_factory, config, tracker, web_session
    ):
        """ Blocking on the worker pool longer than 2xHEARTBEAT will cause
        the broker to close the ReplyListener's connection, and in-flight
        replies will be lost.

        Upon reconnection, any pending RPC requests will raise "disconnected
        waiting for reply".
        """

        container = container_factory(service_cls, config)
        container.start()

        results = []
        exceptions = []

        def cb(worker_ctx, res, exc_info):
            results.append(res)
            exceptions.append(exc_info)
            return res == "OK"

        with entrypoint_waiter(container, 'method', callback=cb):

            with ServiceRpcProxy("downsteam", config) as service_rpc:

                # call an entrypoint that makes a long-running
                # downsteam RPC request
                service_rpc.method.call_async(9)

                # make sufficent other requests that unack'd messages exceeds
                # max workers for longer than 2xHEARTBEAT
                service_rpc.sleep.call_async(10)
                service_rpc.sleep.call_async(11)

        assert results == ["OK"]
        assert exceptions == [None]
