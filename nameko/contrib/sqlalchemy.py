from __future__ import absolute_import

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from nameko.extensions import Dependency

ORM_DB_URIS_KEY = 'ORM_DB_URIS'


class OrmSession(Dependency):
    def __init__(self, declarative_base):
        self.declarative_base = declarative_base
        self.sessions = {}

    def setup(self):
        service_name = self.container.service_name
        decl_base_name = self.declarative_base.__name__
        uri_key = '{}:{}'.format(service_name, decl_base_name)

        db_uris = self.container.config[ORM_DB_URIS_KEY]
        self.db_uri = db_uris[uri_key].format({
            'service_name': service_name,
            'declarative_base_name': decl_base_name,
        })

    def acquire_injection(self, worker_ctx):

        engine = create_engine(self.db_uri)
        Session = sessionmaker(bind=engine)
        session = Session()

        self.sessions[worker_ctx] = session
        return session

    def worker_teardown(self, worker_ctx):
        session = self.sessions.pop(worker_ctx)
        session.close()
