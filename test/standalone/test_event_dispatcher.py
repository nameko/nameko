import pytest
from amqp.exceptions import NotFound
from mock import Mock, patch
from six.moves import queue

import nameko
from nameko.amqp import UndeliverableMessage
from nameko.constants import AMQP_SSL_CONFIG_KEY, LOGIN_METHOD_CONFIG_KEY
from nameko.events import event_handler
from nameko.standalone.events import event_dispatcher, get_event_exchange
from nameko.testing.services import entrypoint_waiter


handler_called = Mock()


class Service(object):
    name = 'destservice'

    @event_handler('srcservice', 'testevent')
    def handler(self, msg):
        handler_called(msg)


def test_dispatch(container_factory, rabbit_config):

    container = container_factory(Service)
    container.start()

    msg = "msg"

    dispatch = event_dispatcher()
    with entrypoint_waiter(container, 'handler', timeout=1):
        dispatch('srcservice', 'testevent', msg)
    handler_called.assert_called_once_with(msg)


class TestMandatoryDelivery(object):
    """ Test and demonstrate mandatory delivery.

    Dispatching an event should raise an exception when mandatory delivery
    is requested and there is no destination queue, as long as publish-confirms
    are enabled.
    """
    @pytest.fixture(autouse=True)
    def event_exchange(self, container_factory, rabbit_config):
        # use a service-based dispatcher to declare an event exchange
        container = container_factory(Service)
        container.start()

    def test_default(self, rabbit_config):
        # events are not mandatory by default;
        # no error when routing to a non-existent handler
        dispatch = event_dispatcher()
        dispatch("srcservice", "bogus", "payload")

    def test_mandatory_delivery(self, rabbit_config):
        # requesting mandatory delivery will result in an exception
        # if there is no bound queue to receive the message
        dispatch = event_dispatcher(mandatory=True)
        with pytest.raises(UndeliverableMessage):
            dispatch("srcservice", "bogus", "payload")

    def test_mandatory_delivery_no_exchange(self, rabbit_config):
        # requesting mandatory delivery will result in an exception
        # if the exchange does not exist
        dispatch = event_dispatcher(mandatory=True)
        with pytest.raises(NotFound):
            dispatch("bogus", "bogus", "payload")

    @patch('nameko.amqp.publish.warnings')
    def test_confirms_disabled(self, warnings, rabbit_config):
        # no exception will be raised if confirms are disabled,
        # even when mandatory delivery is requested,
        # but there will be a warning raised
        dispatch = event_dispatcher(mandatory=True, use_confirms=False)
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
    @pytest.mark.usefixtures("memory_rabbit_config")
    def test_regular_parameters(
        self, parameter, mock_container, producer
    ):
        """ Verify that most parameters can be specified at instantiation time.
        """
        value = Mock()

        dispatch = event_dispatcher(**{parameter: value})

        dispatch("service-name", "event-type", "event-data")
        assert producer.publish.call_args[1][parameter] == value

    @pytest.mark.usefixtures("memory_rabbit_config")
    def test_restricted_parameters(
        self, mock_container, producer
    ):
        """ Verify that providing routing parameters at instantiation
        time has no effect.
        """
        exchange = Mock()
        routing_key = Mock()

        dispatch = event_dispatcher(
            exchange=exchange, routing_key=routing_key)

        service_name = "service-name"
        event_exchange = get_event_exchange(service_name)
        event_type = "event-type"

        dispatch(service_name, event_type, "event-data")

        assert producer.publish.call_args[1]['exchange'] == event_exchange
        assert producer.publish.call_args[1]['routing_key'] == event_type


class TestSSL(object):

    @pytest.fixture(params=["PLAIN", "AMQPLAIN", "EXTERNAL"])
    def login_method(self, request):
        return request.param

    @pytest.fixture(params=[True, False], ids=["use client cert", "no client cert"])
    def use_client_cert(self, request):
        return request.param

    @pytest.fixture
    def rabbit_ssl_config(self, rabbit_ssl_config, use_client_cert, login_method):

        config = {
            # set login method
            LOGIN_METHOD_CONFIG_KEY: login_method
        }

        if use_client_cert is False:
            # remove certificate paths
            config['AMQP_SSL'] = True

        # skip if not a valid combination
        if login_method == "EXTERNAL" and not use_client_cert:
            pytest.skip("EXTERNAL login method requires cert verification")

        with nameko.config.patch(config):
            yield

    @pytest.mark.usefixtures("rabbit_ssl_config")
    def test_event_dispatcher_over_ssl(
        self, container_factory
    ):
        class Service(object):
            name = "service"

            @event_handler("service", "event")
            def echo(self, event_data):
                return event_data

        container = container_factory(Service)
        container.start()

        dispatch = event_dispatcher(
            ssl=nameko.config.get(AMQP_SSL_CONFIG_KEY),
            login_method=nameko.config.get(LOGIN_METHOD_CONFIG_KEY)
        )

        with entrypoint_waiter(container, 'echo') as result:
            dispatch("service", "event", "payload")
        assert result.get() == "payload"
