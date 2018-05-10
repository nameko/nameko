from __future__ import absolute_import

import itertools
from collections import Counter

import pytest
from amqp.exceptions import NotFound
from kombu.messaging import Queue
from mock import ANY, Mock, patch
from six.moves import queue

from nameko.amqp.publish import get_connection
from nameko.containers import WorkerContext
from nameko.events import (
    BROADCAST, SERVICE_POOL, SINGLETON, EventDispatcher, EventHandler,
    EventHandlerConfigurationError, event_handler
)
from nameko.standalone.events import event_dispatcher, get_event_exchange
from nameko.testing.services import entrypoint_waiter
from nameko.testing.utils import DummyProvider
from nameko.testing.waiting import wait_for_call
from nameko.utils.retry import retry


def test_event_dispatcher(mock_container, mock_producer, rabbit_config):

    container = mock_container
    container.config = rabbit_config
    container.service_name = "srcservice"

    service = Mock()
    worker_ctx = WorkerContext(container, service, DummyProvider("dispatch"))

    custom_retry_policy = {'max_retries': 5}

    event_dispatcher = EventDispatcher(retry_policy=custom_retry_policy).bind(
        container, attr_name="dispatch")
    event_dispatcher.setup()

    service.dispatch = event_dispatcher.get_dependency(worker_ctx)
    service.dispatch('eventtype', 'msg')

    headers = event_dispatcher.get_message_headers(worker_ctx)

    expected_args = ('msg',)
    expected_kwargs = {
        'exchange': ANY,
        'routing_key': 'eventtype',
        'headers': headers,
        'declare': event_dispatcher.declare,
        'retry': event_dispatcher.publisher_cls.retry,
        'retry_policy': custom_retry_policy,
        'compression': event_dispatcher.publisher_cls.compression,
        'mandatory': event_dispatcher.publisher_cls.mandatory,
        'expiration': event_dispatcher.publisher_cls.expiration,
        'delivery_mode': event_dispatcher.publisher_cls.delivery_mode,
        'priority': event_dispatcher.publisher_cls.priority,
        'serializer': event_dispatcher.publisher_cls.serializer,
    }

    assert mock_producer.publish.call_count == 1
    args, kwargs = mock_producer.publish.call_args
    assert args == expected_args
    assert kwargs == expected_kwargs
    assert kwargs['exchange'].name == 'srcservice.events'


def test_event_handler(mock_container):

    container = mock_container
    container.service_name = "destservice"

    # test default configuration
    event_handler = EventHandler("srcservice", "eventtype").bind(container,
                                                                 "foobar")
    event_handler.setup()

    assert event_handler.queue.durable is True
    assert event_handler.queue.routing_key == "eventtype"
    assert event_handler.queue.exchange.name == "srcservice.events"

    # test service pool handler
    event_handler = EventHandler(
        "srcservice", "eventtype"
    ).bind(
        container, "foobar"
    )
    event_handler.setup()

    assert event_handler.queue.name == (
        "evt-srcservice-eventtype--destservice.foobar")
    assert event_handler.queue.exclusive is False

    # test broadcast handler with default identifier
    with patch('nameko.events.uuid') as mock_uuid:
        mock_uuid.uuid4().hex = "uuid-value"
        event_handler = EventHandler(
            "srcservice", "eventtype",
            handler_type=BROADCAST, reliable_delivery=False
        ).bind(
            container, "foobar"
        )
        event_handler.setup()

    assert event_handler.queue.name == (
        "evt-srcservice-eventtype--destservice.foobar-{}".format("uuid-value"))
    assert event_handler.queue.exclusive is True

    # test broadcast handler with custom identifier
    class BroadcastEventHandler(EventHandler):
        broadcast_identifier = "testbox"

    event_handler = BroadcastEventHandler(
        "srcservice", "eventtype", handler_type=BROADCAST
    ).bind(
        container, "foobar"
    )
    event_handler.setup()

    assert event_handler.queue.name == (
        "evt-srcservice-eventtype--destservice.foobar-{}".format("testbox"))
    assert event_handler.queue.exclusive is False

    # test singleton handler
    event_handler = EventHandler(
        "srcservice", "eventtype", handler_type=SINGLETON
    ).bind(
        container, "foobar"
    )
    event_handler.setup()

    assert event_handler.queue.name == "evt-srcservice-eventtype"
    assert event_handler.queue.exclusive is False

    # test reliable delivery
    event_handler = EventHandler(
        "srcservice", "eventtype"
    ).bind(
        container, "foobar"
    )
    event_handler.setup()

    assert event_handler.queue.auto_delete is False


class TestReliableDeliveryEventHandlerConfigurationError():

    def test_raises_with_default_broadcast_identity(
        self, mock_container
    ):
        container = mock_container
        container.service_name = "destservice"

        # test broadcast handler with reliable delivery
        with pytest.raises(EventHandlerConfigurationError):
            EventHandler(
                "srcservice", "eventtype",
                handler_type=BROADCAST, reliable_delivery=True
            ).bind(
                container, "foobar"
            )

    def test_no_raise_with_custom_identity(
        self, mock_container
    ):
        container = mock_container
        container.service_name = "destservice"

        # test broadcast handler with reliable delivery and custom identifier
        class BroadcastEventHandler(EventHandler):
            broadcast_identifier = "testbox"

        event_handler = BroadcastEventHandler(
            "srcservice", "eventtype",
            handler_type=BROADCAST, reliable_delivery=True
        ).bind(
            container, "foobar"
        )
        assert event_handler.reliable_delivery is True


# =============================================================================
# INTEGRATION TESTS
# =============================================================================


@pytest.fixture
def tracker():
    return Mock(events=[], workers=[])


def test_service_pooled_events(
    container_factory, rabbit_config, queue_info, tracker
):

    class Base(object):

        @event_handler('srcservice', 'eventtype', handler_type=SERVICE_POOL)
        def handle(self, evt):
            tracker.track()
            tracker.events.append(evt)
            tracker.workers.append(self)

    class FooService(Base):
        name = "foo"

    class BarService(Base):
        name = "bar"

    for service_cls in (FooService, FooService, BarService):
        container = container_factory(service_cls, rabbit_config)
        container.start()

    # foo service pool queue should have two consumers
    foo_queue_name = "evt-srcservice-eventtype--foo.handle"
    assert queue_info(foo_queue_name).consumer_count == 2

    # bar service pool queue should have one consumer
    bar_queue_name = "evt-srcservice-eventtype--bar.handle"
    assert queue_info(bar_queue_name).consumer_count == 1

    dispatch = event_dispatcher(rabbit_config)

    count = itertools.count(start=1)
    with wait_for_call(
        tracker, 'track', callback=lambda *args: next(count) == 2
    ):
        dispatch("srcservice", "eventtype", "msg")

    # a total of two events should be received
    assert len(tracker.events) == 2

    # exactly one instance of each service should have been created
    # each should have received an event
    assert len(tracker.workers) == 2
    assert {type(worker) for worker in tracker.workers} == {
        FooService, BarService
    }


def test_service_pooled_events_multiple_handlers(
    container_factory, rabbit_config, queue_info, tracker
):

    class Service(object):
        name = "double"

        def handle(self, evt):
            tracker.events.append(evt)
            tracker.workers.append(self)

        @event_handler('srcservice', 'eventtype', handler_type=SERVICE_POOL)
        def handle_1(self, evt):
            self.handle(evt)

        @event_handler('srcservice', 'eventtype', handler_type=SERVICE_POOL)
        def handle_2(self, evt):
            self.handle(evt)

    container = container_factory(Service, rabbit_config)
    container.start()

    # we should have two queues with a consumer each
    queue_1_name = "evt-srcservice-eventtype--double.handle_1"
    assert queue_info(queue_1_name).consumer_count == 1
    queue_2_name = "evt-srcservice-eventtype--double.handle_2"
    assert queue_info(queue_2_name).consumer_count == 1

    dispatch = event_dispatcher(rabbit_config)

    with entrypoint_waiter(container, 'handle_1'):
        with entrypoint_waiter(container, 'handle_2'):
            dispatch("srcservice", "eventtype", "msg")

    # each handler should have received the event
    assert len(tracker.events) == 2

    # two worker instances would have been created to deal with the handling
    assert len(tracker.workers) == 2
    assert {type(worker) for worker in tracker.workers} == {Service}


def test_singleton_events(
    container_factory, rabbit_config, queue_info, tracker
):

    class Base(object):

        @event_handler('srcservice', 'eventtype', handler_type=SINGLETON)
        def handle(self, evt):
            tracker.track()
            tracker.events.append(evt)
            tracker.workers.append(self)

    class FooService(Base):
        name = "foo"

    class BarService(Base):
        name = "bar"

    for service_cls in (FooService, FooService, BarService):
        container = container_factory(service_cls, rabbit_config)
        container.start()

    # the singleton queue should have three consumers
    assert queue_info("evt-srcservice-eventtype").consumer_count == 3

    dispatch = event_dispatcher(rabbit_config)

    count = itertools.count(start=1)
    with wait_for_call(
        tracker, 'track', callback=lambda *args: next(count) == 1
    ):
        dispatch("srcservice", "eventtype", "msg")

    # exactly one event should have been received
    assert len(tracker.events) == 1

    # one lucky handler should have received the event
    assert len(tracker.workers) == 1
    assert {type(worker) for worker in tracker.workers}.issubset(
        {FooService, BarService}
    )


def test_broadcast_events(
    container_factory, rabbit_config, queue_info, tracker, rabbit_manager
):

    class Base(object):

        @event_handler(
            'srcservice', 'eventtype',
            handler_type=BROADCAST, reliable_delivery=False
        )
        def handle(self, evt):
            tracker.track()
            tracker.events.append(evt)
            tracker.workers.append(self)

    class FooService(Base):
        name = "foo"

    class BarService(Base):
        name = "bar"

    for service_cls in (FooService, FooService, BarService):
        container = container_factory(service_cls, rabbit_config)
        container.start()

    vhost = rabbit_config['vhost']

    # each broadcast queue should have one consumer
    queues = rabbit_manager.get_queues(vhost)
    queue_names = [queue['name'] for queue in queues
                   if queue['name'].startswith("evt-srcservice-eventtype-")]

    assert len(queue_names) == 3
    for name in queue_names:
        # TODO can use queue_info here once
        # https://github.com/nameko/nameko/pull/484 lands and we drop the
        # exclusive flag on broadcast handlers
        # assert queue_info(name).consumer_count == 1
        queue = rabbit_manager.get_queue(vhost, name)
        assert len(queue['consumer_details']) == 1

    dispatch = event_dispatcher(rabbit_config)

    count = itertools.count(start=1)
    with wait_for_call(
        tracker, 'track', callback=lambda *args: next(count) == 3
    ):
        dispatch("srcservice", "eventtype", "msg")

    # a total of three events should be received
    assert len(tracker.events) == 3

    # all three handlers should receive the event, but they're only of two
    # different types
    assert len(tracker.workers) == 3
    worker_counts = Counter([type(worker) for worker in tracker.workers])
    assert worker_counts[FooService] == 2
    assert worker_counts[BarService] == 1


def test_requeue_on_error(
    container_factory, rabbit_config, queue_info, tracker
):

    class Service(object):
        name = "requeue"

        @event_handler('srcservice', 'eventtype', requeue_on_error=True)
        def handle(self, evt):
            tracker.track()
            tracker.events.append(evt)
            tracker.workers.append(self)
            raise Exception("Error")

    container = container_factory(Service, rabbit_config)
    container.start()

    # the queue should been created and have one consumer
    queue_name = "evt-srcservice-eventtype--requeue.handle"
    assert queue_info(queue_name).consumer_count == 1

    dispatch = event_dispatcher(rabbit_config)

    counter = itertools.count(start=1)
    with entrypoint_waiter(
        container, 'handle', callback=lambda *args: next(counter) > 1
    ):
        dispatch("srcservice", "eventtype", "msg")

    # the event will be received multiple times as it gets requeued and then
    # consumed again
    assert len(tracker.events) > 1

    # multiple instances of the service should have been instantiated
    assert len(tracker.workers) > 1


def test_reliable_delivery(
    container_factory, rabbit_config, queue_info, tracker
):
    """ Events sent to queues declared by ``reliable_delivery`` handlers
    should be received even if no service was listening when they were
    dispatched.
    """
    class Service(object):
        name = "reliable"

        @event_handler('srcservice', 'eventtype', reliable_delivery=True)
        def handle(self, evt):
            tracker.track()
            tracker.events.append(evt)
            tracker.workers.append(self)

    container = container_factory(Service, rabbit_config)
    container.start()

    # test queue created, with one consumer
    queue_name = "evt-srcservice-eventtype--reliable.handle"
    assert queue_info(queue_name).consumer_count == 1

    dispatch = event_dispatcher(rabbit_config)

    # dispatch an event
    with entrypoint_waiter(container, 'handle'):
        dispatch("srcservice", "eventtype", "msg_1")

    assert tracker.events == ["msg_1"]

    # stop container, check queue still exists, without consumers
    container.stop()

    @retry
    def consumer_removed():
        assert queue_info(queue_name).consumer_count == 0
    consumer_removed()

    # dispatch another event while nobody is listening
    dispatch("srcservice", "eventtype", "msg_2")

    # verify the message gets queued
    assert queue_info(queue_name).message_count == 1

    # start another container
    container = container_factory(Service, rabbit_config)
    with entrypoint_waiter(container, 'handle'):
        container.start()

    # check the new service to collects the pending event
    assert tracker.events == ["msg_1", "msg_2"]


def test_unreliable_delivery(
    container_factory, rabbit_config, queue_info, tracker
):
    """ Events sent to queues declared by non- ``reliable_delivery`` handlers
    should be lost if no service was listening when they were dispatched.
    """

    class UnreliableService(object):
        name = "unreliable"

        @event_handler('srcservice', 'eventtype', reliable_delivery=False)
        def handle(self, evt):
            tracker.track()
            tracker.events.append(evt)
            tracker.workers.append(self)

    unreliable_container = container_factory(UnreliableService, rabbit_config)
    unreliable_container.start()

    class ReliableService(object):
        name = "reliable"

        @event_handler('srcservice', 'eventtype', reliable_delivery=True)
        def handle(self, evt):
            tracker.track()
            tracker.events.append(evt)
            tracker.workers.append(self)

    reliable_container = container_factory(ReliableService, rabbit_config)
    reliable_container.start()

    # test unreliable queue created, with one consumer
    queue_name = "evt-srcservice-eventtype--unreliable.handle"
    assert queue_info(queue_name).consumer_count == 1

    dispatch = event_dispatcher(rabbit_config)

    # dispatch an event
    count = itertools.count(start=1)
    with wait_for_call(
        tracker, 'track', callback=lambda *args: next(count) == 2
    ):
        dispatch("srcservice", "eventtype", "msg_1")

    assert tracker.events == ["msg_1", "msg_1"]

    # test that both services received it
    assert len(tracker.workers) == 2
    worker_counts = Counter([type(worker) for worker in tracker.workers])
    assert worker_counts[ReliableService] == 1
    assert worker_counts[UnreliableService] == 1

    # stop container, test queue deleted
    unreliable_container.stop()

    @retry
    def queue_removed():
        with pytest.raises(NotFound):
            queue_info(queue_name)
    queue_removed()

    # dispatch a second event while nobody is listening
    count = itertools.count(start=1)
    with wait_for_call(
        tracker, 'track', callback=lambda *args: next(count) == 1
    ):
        dispatch("srcservice", "eventtype", "msg_2")

    # start another container
    unreliable_container = container_factory(UnreliableService, rabbit_config)
    unreliable_container.start()

    # verify the queue is recreated, with one consumer
    assert queue_info(queue_name).consumer_count == 1

    # dispatch a third event
    count = itertools.count(start=1)
    with wait_for_call(
        tracker, 'track', callback=lambda *args: next(count) == 2
    ):
        dispatch("srcservice", "eventtype", "msg_3")

    # verify that the "unreliable" handler didn't receive the message sent
    # when there wasn't an instance running
    assert tracker.events == ["msg_1", "msg_1", "msg_2", "msg_3", "msg_3"]
    worker_counts = Counter([type(worker) for worker in tracker.workers])
    assert worker_counts[ReliableService] == 3
    assert worker_counts[UnreliableService] == 2


def test_dispatch_to_rabbit(rabbit_manager, rabbit_config, mock_container):

    vhost = rabbit_config['vhost']

    container = mock_container
    container.shared_extensions = {}
    container.service_name = "srcservice"
    container.config = rabbit_config

    service = Mock()
    worker_ctx = WorkerContext(container, service, DummyProvider())

    dispatcher = EventDispatcher().bind(container, 'dispatch')
    dispatcher.setup()
    dispatcher.start()

    # we should have an exchange but no queues
    exchanges = rabbit_manager.get_exchanges(vhost)
    queues = rabbit_manager.get_queues(vhost)
    assert "srcservice.events" in [exchange['name'] for exchange in exchanges]
    assert queues == []

    # manually add a queue to capture the events
    rabbit_manager.create_queue(vhost, "event-sink", auto_delete=True)
    rabbit_manager.create_queue_binding(
        vhost, "srcservice.events", "event-sink", routing_key="eventtype")

    service.dispatch = dispatcher.get_dependency(worker_ctx)
    service.dispatch("eventtype", "msg")

    # test event receieved on manually added queue
    messages = rabbit_manager.get_messages(vhost, "event-sink")
    assert ['"msg"'] == [msg['payload'] for msg in messages]


class TestConfigurability(object):
    """
    Test and demonstrate configuration options for the EventDispatcher
    """

    @pytest.yield_fixture
    def get_producer(self):
        with patch('nameko.amqp.publish.get_producer') as get_producer:
            yield get_producer

    @pytest.fixture
    def producer(self, get_producer):
        producer = get_producer().__enter__.return_value
        # make sure we don't raise UndeliverableMessage if mandatory is True
        producer.channel.returned_messages.get_nowait.side_effect = queue.Empty
        return producer

    @pytest.mark.parametrize("parameter", [
        # delivery options
        'delivery_mode', 'mandatory', 'priority', 'expiration',
        # message options
        'serializer', 'compression',
        # retry policy
        'retry', 'retry_policy',
        # other arbitrary publish kwargs
        'correlation_id', 'user_id', 'bogus_param'
    ])
    def test_regular_parameters(
        self, parameter, mock_container, producer
    ):
        """ Verify that most parameters can be specified at instantiation time.
        """
        mock_container.config = {'AMQP_URI': 'memory://localhost'}
        mock_container.service_name = "service"

        worker_ctx = Mock()
        worker_ctx.context_data = {}

        value = Mock()

        dispatcher = EventDispatcher(
            **{parameter: value}
        ).bind(mock_container, "dispatch")
        dispatcher.setup()

        dispatch = dispatcher.get_dependency(worker_ctx)

        dispatch("event-type", "event-data")
        assert producer.publish.call_args[1][parameter] == value

    @pytest.mark.usefixtures('predictable_call_ids')
    def test_headers(self, mock_container, producer):
        """ Headers can be provided at instantiation time, and are merged with
        Nameko headers.
        """
        mock_container.config = {
            'AMQP_URI': 'memory://localhost'
        }
        mock_container.service_name = "service"

        # use a real worker context so nameko headers are generated
        service = Mock()
        entrypoint = Mock(method_name="method")
        worker_ctx = WorkerContext(
            mock_container, service, entrypoint, data={'context': 'data'}
        )

        nameko_headers = {
            'nameko.context': 'data',
            'nameko.call_id_stack': ['service.method.0'],
        }

        value = {'foo': Mock()}

        dispatcher = EventDispatcher(
            **{'headers': value}
        ).bind(mock_container, "dispatch")
        dispatcher.setup()

        dispatch = dispatcher.get_dependency(worker_ctx)

        def merge_dicts(base, *updates):
            merged = base.copy()
            [merged.update(update) for update in updates]
            return merged

        dispatch("event-type", "event-data")
        assert producer.publish.call_args[1]['headers'] == merge_dicts(
            nameko_headers, value
        )

    def test_restricted_parameters(
        self, mock_container, producer
    ):
        """ Verify that providing routing parameters at instantiation
        time has no effect.
        """
        mock_container.config = {'AMQP_URI': 'memory://localhost'}
        mock_container.service_name = "service"

        worker_ctx = Mock()
        worker_ctx.context_data = {}

        exchange = Mock()
        routing_key = Mock()

        dispatcher = EventDispatcher(
            exchange=exchange,
            routing_key=routing_key,
        ).bind(mock_container, "dispatch")
        dispatcher.setup()

        dispatch = dispatcher.get_dependency(worker_ctx)

        event_exchange = get_event_exchange("service")
        event_type = "event-type"

        dispatch(event_type, "event-data")

        assert producer.publish.call_args[1]['exchange'] == event_exchange
        assert producer.publish.call_args[1]['routing_key'] == event_type
