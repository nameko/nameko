""" Service unit testing best practice, with a provided injection.
"""

import pytest
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from nameko.rpc import rpc
from nameko.contrib.sqlalchemy import orm_session
from nameko.testing.services import worker_factory


Base = declarative_base()


class Result(Base):
    __tablename__ = 'model'
    id = Column(Integer, primary_key=True)
    value = Column(String(64))


class Service(object):
    db = orm_session(Base)

    @rpc
    def save(self, value):
        result = Result(value=value)
        self.db.add(result)
        self.db.commit()

# =============================================================================
# Begin test
# =============================================================================


@pytest.fixture
def session():

    # create sqlite database and session
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def test_service(session):

    # create instance, providing the real session for the ``db`` injection
    service = worker_factory(Service, db=session)

    # verify ``save`` logic by querying the real database
    service.save("helloworld")
    assert session.query(Result.value).all() == [("helloworld",)]
