from mock import Mock
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base

from nameko.db import ORMSession
from nameko.events import event_handler, EventDispatcher, Event
from nameko.rpc import rpc, Service
from nameko.timer import timer
from nameko.testing.utils import wait_for_call

DeclBase = declarative_base(name='foo_base')


class FooModel(DeclBase):
    __tablename__ = 'spam'
    id = Column(Integer, primary_key=True)
    data = Column(String)


class SpamEvent(Event):
    type='spam'


class FooService(object):
    name = "foo_service"

    foo_session = ORMSession(DeclBase)
    dispatch_event = EventDispatcher()
    foo_service = Service()

    @timer(interval=1)
    def handle_timer(self):
        ham = 'ham'
        self.dispatch_event(SpamEvent(ham))

    @event_handler('foo_service', 'spam')
    def handle_spam(self, evt_data):
        ham = self.foo_service.spam(evt_data)
        handle_spam_called(ham)

    @rpc
    def spam(self, ham):
        self.foo_session.add(FooModel(data=ham))
        self.foo_session.flush()
        return ham + ' & eggs'



handle_spam_called = Mock()


def test_example_service(container_factory, rabbit_config):
    db_uri = 'sqlite:///foobar.sqlite'
    engine = create_engine(db_uri)
    FooModel.metadata.create_all(engine)

    config = {
        'amqp_uri': rabbit_config['amqp_uri'],
        'orm_db_uris': {
            'foo_service:foo_base': db_uri
        }
    }

    container = container_factory(FooService, config)
    container.start()

    with wait_for_call(5, handle_spam_called) as handle_spam:
        handle_spam.assert_called_with('ham & eggs')

    entries = list(engine.execute('SELECT data FROM spam LIMIT 1'))
    assert entries ==  [('ham',)]

    container.stop()