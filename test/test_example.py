import tempfile

from kombu import Exchange, Queue
from mock import Mock
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base

from nameko.contrib.sqlalchemy import orm_session, ORM_DB_URIS_KEY
from nameko.events import event_handler, event_dispatcher, Event
from nameko.messaging import publisher, consume
from nameko.rpc import rpc, rpc_proxy
from nameko.timer import timer
from nameko.testing.utils import wait_for_call

DeclBase = declarative_base(name='foo_base')


class FooModel(DeclBase):
    __tablename__ = 'spam'
    id = Column(Integer, primary_key=True)
    data = Column(String)


class SpamEvent(Event):
    type = 'spam'


foobar_ex = Exchange('foobar_ex', durable=False)
foobar_queue = Queue('foobar_queue', exchange=foobar_ex, durable=False)


class FooService(object):
    name = 'foo-service'

    foo_session = orm_session(DeclBase)
    dispatch_event = event_dispatcher()
    foo_service = rpc_proxy('foo-service')
    publish = publisher(queue=foobar_queue)

    @timer(interval=1)
    def handle_timer(self):
        ham = 'ham'
        self.dispatch_event(SpamEvent(ham))

    @event_handler('foo-service', 'spam')
    def handle_spam(self, evt_data):
        ham = self.foo_service.spam(evt_data)
        handle_spam_called(ham)

    @rpc
    def spam(self, ham):
        self.publish("message")
        self.foo_session.add(FooModel(data=ham))
        self.foo_session.commit()
        self.foo_session.flush()
        return ham + ' & eggs'

    @consume(queue=foobar_queue)
    def foo(self, msg):
        handle_foo_called(msg)


handle_spam_called = Mock()
handle_foo_called = Mock()


def test_example_service(container_factory, rabbit_config):

    db_uri = 'sqlite:///{}'.format(tempfile.NamedTemporaryFile().name)
    engine = create_engine(db_uri)
    FooModel.metadata.create_all(engine)

    config = {
        ORM_DB_URIS_KEY: {
            'foo-service:foo_base': db_uri
        }
    }
    config.update(rabbit_config)

    container = container_factory(FooService, config)
    container.start()

    with wait_for_call(5, handle_spam_called) as handle_spam:
        handle_spam.assert_called_with('ham & eggs')

    with wait_for_call(5, handle_foo_called) as handle_foo:
        handle_foo.assert_called_with('message')

    entries = list(engine.execute('SELECT data FROM spam LIMIT 1'))
    assert entries == [('ham',)]

    container.stop()
