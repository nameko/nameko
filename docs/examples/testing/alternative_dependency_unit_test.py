""" Service unit testing best practice, with an alternative dependency.
"""

import pytest
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from nameko.rpc import rpc
from nameko.testing.services import worker_factory

# using community extension from http://pypi.python.org/pypi/nameko-sqlalchemy
from nameko_sqlalchemy import Session


Base = declarative_base()


class Result(Base):
    __tablename__ = 'model'
    id = Column(Integer, primary_key=True)
    value = Column(String(64))


class Service(object):
    """ Service under test
    """
    name = "service"

    db = Session(Base)

    @rpc
    def save(self, value):
        result = Result(value=value)
        self.db.add(result)
        self.db.commit()


@pytest.fixture
def session():
    """ Create a test database and session
    """
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    session_cls = sessionmaker(bind=engine)
    return session_cls()


def test_service(session):

    # create instance, providing the test database session
    service = worker_factory(Service, db=session)

    # verify ``save`` logic by querying the test database
    service.save("helloworld")
    assert session.query(Result.value).all() == [("helloworld",)]
