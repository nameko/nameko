from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import Session

from nameko.db import ORMSession
from nameko.service import ServiceContext, WorkerContext



def test_db():

    DeclBase = declarative_base(name='spam_base')

    class FooModel(DeclBase):
        __tablename__ = 'spam'
        id = Column(Integer, primary_key=True)


    class FooService(object):
        foo_session = ORMSession(DeclBase)


    FooService.foo_session.name = 'foo_session'

    config = {
        'orm_db_uris':{
            'foo_service:spam_base': 'sqlite:///:memory:'
        }
    }

    service = FooService()

    srv_ctx = ServiceContext('foo_service', None, None, config=config)
    worker_ctx = WorkerContext(srv_ctx, service, 'shrub')

    # we pretend to be a service container and initiate some lifecycle handling
    FooService.foo_session.call_setup(worker_ctx)

    session = service.foo_session
    assert isinstance(session, Session)

    session.add(FooModel())
    assert session.new

    FooService.foo_session.call_teardown(worker_ctx)
    # if we had not closed the session we would still have new objects
    assert not session.new

    # because the actual session was deleted, we should get and instance
    # of the declared ORMSession from the service class
    assert service.foo_session is FooService.foo_session

