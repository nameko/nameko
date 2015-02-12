import tempfile

from kombu import Exchange, Queue
from mock import Mock
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base

from nameko.contrib.sqlalchemy import OrmSession, ORM_DB_URIS_KEY
from nameko.events import event_handler, EventDispatcher
from nameko.messaging import Publisher, consume
from nameko.rpc import rpc, RpcProxy
from nameko.timer import timer
from nameko.testing.services import entrypoint_waiter

DeclBase = declarative_base(name='foo_base')


class FooModel(DeclBase):
    __tablename__ = 'spam'
    id = Column(Integer, primary_key=True)
    data = Column(String)


foobar_ex = Exchange('foobar_ex', durable=False)
foobar_queue = Queue('foobar_queue', exchange=foobar_ex, durable=False)


class FooService(object):
    name = 'foo-service'

    foo_session = OrmSession(DeclBase)
    dispatch_event = EventDispatcher()
    foo_service = RpcProxy('foo-service')
    publish = Publisher(queue=foobar_queue)

    @timer(interval=1)
    def handle_timer(self):
        ham = 'ham'
        self.dispatch_event('spam', ham)

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

    spam_waiter = entrypoint_waiter(container, 'handle_spam')
    foo_waiter = entrypoint_waiter(container, 'foo')
    with spam_waiter, foo_waiter:
        container.start()

    handle_spam_called.assert_called_with('ham & eggs')
    handle_foo_called.assert_called_with('message')

    entries = list(engine.execute('SELECT data FROM spam LIMIT 1'))
    assert entries == [('ham',)]

    container.stop()
