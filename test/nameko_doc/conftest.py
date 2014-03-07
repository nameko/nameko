from path import path
import pytest
from tempfile import mkdtemp


@pytest.fixture(scope='session')
def example_projects_source():
    test_root = path(__file__).parent
    example_projects_source = test_root.joinpath('example_projects')
    return example_projects_source


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


@pytest.fixture(scope='session')
def copy_example(example_projects_source, temp_folder_factory):
    def copy(example_name):
        to_place = temp_folder_factory(reserve_only=True)
        original = example_projects_source.joinpath(example_name)
        original.copytree(to_place)
        return path(to_place)
    return copy
