import os
import uuid

import pytest


@pytest.yield_fixture
def testdir(testdir):
    """ Wrap pytester's ``testdir`` fixture to include coverage from
    the generated tests.
    """
    rc_path = os.path.join(
        os.path.dirname(__file__), os.pardir, '.coveragerc'
    )
    os.environ['COVERAGE_PROCESS_START'] = os.path.normpath(rc_path)

    testdir.makepyfile(sitecustomize="""
        import coverage; coverage.process_startup()
    """)

    yield testdir

    ident = uuid.uuid4()
    dst = os.path.join(testdir._olddir.strpath, '.coverage.{}'.format(ident))
    os.rename(os.path.join(testdir.tmpdir.strpath, '.coverage'), dst)
