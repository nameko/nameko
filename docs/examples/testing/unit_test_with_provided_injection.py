""" Service unit testing best practice, with a provided injection.
"""

from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from nameko.rpc import rpc
from nameko.contrib.sqlalchemy import orm_session
from nameko.testing.service.unit import instance_factory

# define a simple declarative base and model for sqlalchemy
DeclBase = declarative_base(name='example_base')


class Result(DeclBase):
    __tablename__ = 'model'
    id = Column(Integer, primary_key=True)
    value = Column(String(64))


# service under test writes to a database
class Service(object):
    db = orm_session(DeclBase)

    @rpc
    def save(self, value):
        result = Result(value=value)
        self.db.add(result)
        self.db.commit()

# create sqlite database and session
engine = create_engine('sqlite:///:memory:')
DeclBase.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()


def unit_test_with_provided_injection():

    # create instance, providing the real session for the ``db`` injection
    service = instance_factory(Service, db=session)

    # verify ``save`` logic by querying the real database
    service.save("helloworld")
    assert session.query(Result.value).all() == [("helloworld",)]
