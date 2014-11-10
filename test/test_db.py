from eventlet.greenpool import GreenPile
from mock import Mock
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import Session

from nameko.contrib.sqlalchemy import orm_session, OrmSession, ORM_DB_URIS_KEY
from nameko.containers import WorkerContext
from nameko.testing.utils import DummyProvider

CONCURRENT_REQUESTS = 10

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


def test_concurrency():

    container = Mock()
    container.config = config
    container.service_name = "fooservice"

    entrypoint = DummyProvider()
    service_instance = Mock()

    def inject(worker_ctx):
        orm_session = OrmSession(DeclBase)
        orm_session.container = container
        return orm_session.acquire_injection(worker_ctx)

    # get injections concurrently
    pile = GreenPile()
    for _ in xrange(CONCURRENT_REQUESTS):
        worker_ctx = WorkerContext(container, service_instance, entrypoint)
        pile.spawn(inject, worker_ctx)
    results = set(pile)

    # injections should all be unique
    assert len(results) == CONCURRENT_REQUESTS


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
