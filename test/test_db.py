from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import Session

from nameko.contrib.sqlalchemy import OrmSession, ORM_DB_URIS_KEY
from nameko.containers import WorkerContext
from nameko.testing.utils import DummyProvider, get_extension

CONCURRENT_REQUESTS = 10

DeclBase = declarative_base(name='spam_base')


class FooModel(DeclBase):
    __tablename__ = 'spam'
    id = Column(Integer, primary_key=True)


class FooService(object):
    session = OrmSession(DeclBase)


config = {
    ORM_DB_URIS_KEY: {
        'fooservice:spam_base': 'sqlite:///:memory:'
    }
}


def test_db(container_factory):

    container = container_factory(FooService, config)
    container.start()
    orm_session = get_extension(container, OrmSession)

    # fake instance creation and dependency injection
    service = FooService()
    worker_ctx = WorkerContext(container, service, DummyProvider())
    service.session = orm_session.get_dependency(worker_ctx)

    assert isinstance(service.session, Session)

    session = service.session
    session.add(FooModel())
    assert session.new

    orm_session.worker_teardown(worker_ctx)
    # if we had not closed the session we would still have new objects
    assert not session.new
