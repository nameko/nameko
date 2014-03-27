from path import path
import pytest
from tempfile import mkdtemp


@pytest.yield_fixture(scope='session')
def temp_folder_factory():
    created = set()

    def factory(reserve_only=False):
        temp_dir = mkdtemp()
        if reserve_only:
            # It'll still be cleaned at the end
            path(temp_dir).rmtree(ignore_errors=True)
        created.add(temp_dir)
        return path(temp_dir)

    yield factory

    for to_delete in created:
        path(to_delete).rmtree(ignore_errors=True)


def assert_entities_equal(entity, other):
    assert isinstance(other, type(entity))
    assert entity.__dict__ == other.__dict__
