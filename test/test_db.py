from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import Session

from nameko.contrib.sqlalchemy import orm_session, ORM_DB_URIS_KEY
from nameko.containers import WorkerContext
from nameko.testing.utils import DummyProvider


DeclBase = declarative_base(name='spam_base')


class FooModel(DeclBase):
    __tablename__ = 'spam'
    id = Column(Integer, primary_key=True)


class FooService(object):
    session = orm_session(DeclBase)


config = {
    ORM_DB_URIS_KEY: {
        'fooservice:spam_base': 'sqlite:///:memory:'
    }
}


def test_db(container_factory):

    container = container_factory(FooService, config)
    provider = next(iter(container.dependencies.injections))

    # fake instance creation and provider injection
    service = FooService()
    worker_ctx = WorkerContext(container, service, DummyProvider())
    provider.inject(worker_ctx)

    assert isinstance(service.session, Session)

    session = service.session
    session.add(FooModel())
    assert session.new

    provider.worker_teardown(worker_ctx)
    provider.release(worker_ctx)
    # if we had not closed the session we would still have new objects
    assert not session.new

    # teardown removes the injection
    assert not isinstance(service.session, Session)
