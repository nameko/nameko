from mock import Mock, patch, call
from six.moves import queue
import itertools
import pytest

from amqp.exceptions import NotFound
from kombu.common import maybe_declare

from nameko.amqp import UndeliverableMessage
from nameko.amqp.publish import get_connection
from nameko.events import event_handler
from nameko.extensions import DependencyProvider
from nameko.standalone.events import event_dispatcher, get_event_exchange
from nameko.testing.services import entrypoint_waiter
from nameko.testing.utils import unpack_mock_call


class TestEventDispatcher(object):

    @pytest.yield_fixture
    def predictable_call_ids(self, predictable_call_ids):
        with patch('nameko.standalone.events.uuid') as uuid:
            uuid.uuid4.side_effect = itertools.count()
            yield uuid

    @pytest.fixture
    def handler_tracker(self):
        return Mock()

    @pytest.fixture
    def context_tracker(self):
        return Mock()

    @pytest.fixture
    def service_cls(self, context_tracker, handler_tracker):

        class ContextTracker(DependencyProvider):

            def worker_setup(self, worker_ctx):
                context_tracker(worker_ctx)

        class Service(object):
            name = 'destservice'

            context_tracker = ContextTracker()

            @event_handler('srcservice', 'testevent')
            def handler(self, msg):
                handler_tracker(msg)

        return Service

    def test_dispatch(
        self, service_cls, container_factory, rabbit_config, handler_tracker
    ):
        container = container_factory(service_cls, rabbit_config)
        container.start()

        msg = "msg"

        dispatch = event_dispatcher(rabbit_config)

        with entrypoint_waiter(container, 'handler', timeout=1):
            dispatch('srcservice', 'testevent', msg)
        assert handler_tracker.call_args_list == [call(msg)]

    def test_context_data(
        self, service_cls, container_factory, rabbit_config, context_tracker
    ):
        container = container_factory(service_cls, rabbit_config)
        container.start()

        context_data = {'foo': 'bar'}

        dispatch = event_dispatcher(rabbit_config, context_data=context_data)

        with entrypoint_waiter(container, 'handler', timeout=1):
            dispatch('srcservice', 'testevent', 'msg')

        worker_ctx = unpack_mock_call(context_tracker.call_args).positional[0]
        assert worker_ctx.context_data['foo'] == 'bar'

    @pytest.mark.usefixtures('predictable_call_ids')
    def test_call_id(
        self, service_cls, container_factory, rabbit_config, context_tracker
    ):
        container = container_factory(service_cls, rabbit_config)
        container.start()

        dispatch = event_dispatcher(rabbit_config)

        with entrypoint_waiter(container, 'handler', timeout=1):
            dispatch('srcservice', 'testevent', 'msg')

        worker_ctx = unpack_mock_call(context_tracker.call_args).positional[0]
        assert worker_ctx.context_data['call_id_stack'] == [
            'standalone_event_dispatcher.srcservice.testevent.0',
            'destservice.handler.0'
        ]

    @pytest.mark.usefixtures('predictable_call_ids')
    def test_call_id_custom_prefix(
        self, service_cls, container_factory, rabbit_config, context_tracker
    ):
        container = container_factory(service_cls, rabbit_config)
        container.start()

        dispatch = event_dispatcher(
            rabbit_config, call_id_prefix="custom_prefix"
        )
        with entrypoint_waiter(container, 'handler', timeout=1):
            dispatch('srcservice', 'testevent', 'msg')

        worker_ctx = unpack_mock_call(context_tracker.call_args).positional[0]
        assert worker_ctx.context_data['call_id_stack'] == [
            'custom_prefix.srcservice.testevent.0',
            'destservice.handler.0'
        ]


class TestMandatoryDelivery(object):
    """ Test and demonstrate mandatory delivery.

    Dispatching an event should raise an exception when mandatory delivery
    is requested and there is no destination queue, as long as publish-confirms
    are enabled.
    """
    @pytest.fixture(autouse=True)
    def event_exchange(self, rabbit_config):
        exchange = get_event_exchange('srcservice')
        with get_connection(rabbit_config['AMQP_URI']) as conn:
            maybe_declare(exchange, conn)

    def test_default(self, rabbit_config):
        # events are not mandatory by default;
        # no error when routing to a non-existent handler
        dispatch = event_dispatcher(rabbit_config)
        dispatch("srcservice", "bogus", "payload")

    def test_mandatory_delivery(self, rabbit_config):
        # requesting mandatory delivery will result in an exception
        # if there is no bound queue to receive the message
        dispatch = event_dispatcher(rabbit_config, mandatory=True)
        with pytest.raises(UndeliverableMessage):
            dispatch("srcservice", "bogus", "payload")

    def test_mandatory_delivery_no_exchange(self, rabbit_config):
        # requesting mandatory delivery will result in an exception
        # if the exchange does not exist
        dispatch = event_dispatcher(rabbit_config, mandatory=True)
        with pytest.raises(NotFound):
            dispatch("bogus", "bogus", "payload")

    @patch('nameko.amqp.publish.warnings')
    def test_confirms_disabled(self, warnings, rabbit_config):
        # no exception will be raised if confirms are disabled,
        # even when mandatory delivery is requested,
        # but there will be a warning raised
        dispatch = event_dispatcher(
            rabbit_config, mandatory=True, use_confirms=False
        )
        dispatch("srcservice", "bogus", "payload")
        assert warnings.warn.called


class TestConfigurability(object):
    """
    Test and demonstrate configuration options for the standalone dispatcher
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
        config = {'AMQP_URI': 'memory://localhost'}

        value = Mock()

        dispatch = event_dispatcher(config, **{parameter: value})

        dispatch("service-name", "event-type", "event-data")
        assert producer.publish.call_args[1][parameter] == value

    def test_restricted_parameters(
        self, mock_container, producer
    ):
        """ Verify that providing routing parameters at instantiation
        time has no effect.
        """
        config = {'AMQP_URI': 'memory://localhost'}

        exchange = Mock()
        routing_key = Mock()

        dispatch = event_dispatcher(
            config, exchange=exchange, routing_key=routing_key
        )

        service_name = "service-name"
        event_exchange = get_event_exchange(service_name)
        event_type = "event-type"

        dispatch(service_name, event_type, "event-data")

        assert producer.publish.call_args[1]['exchange'] == event_exchange
        assert producer.publish.call_args[1]['routing_key'] == event_type
