from __future__ import absolute_import

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from nameko.dependencies import InjectionProvider, injection, DependencyFactory

ORM_DB_URIS_KEY = 'ORM_DB_URIS'


class OrmSession(InjectionProvider):
    def __init__(self, declarative_base):
        self.declarative_base = declarative_base
        self.sessions = {}

    def acquire_injection(self, worker_ctx):
        service_name = self.container.service_name
        decl_base_name = self.declarative_base.__name__
        uri_key = '{}:{}'.format(service_name, decl_base_name)

        db_uris = worker_ctx.config[ORM_DB_URIS_KEY]
        db_uri = db_uris[uri_key].format({
            'service_name': service_name,
            'declarative_base_name': decl_base_name,
        })

        engine = create_engine(db_uri)
        Session = sessionmaker(bind=engine)
        session = Session()

        self.sessions[worker_ctx] = session
        return session

    def worker_teardown(self, worker_ctx):
        session = self.sessions.pop(worker_ctx)
        session.close()


@injection
def orm_session(declarative_base):
    return DependencyFactory(OrmSession, declarative_base)
