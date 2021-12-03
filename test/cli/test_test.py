from textwrap import dedent
import subprocess


def test_test_pass(tmpdir, capsys):

    tmpdir.join('__init__.py')
    testfile = tmpdir.join('test_test_pass.py')
    testfile.write(dedent("""
        def test_foo():
            assert True
    """))

    proc = subprocess.Popen(
        ["nameko", "test", testfile.strpath], stdout=subprocess.PIPE
    )
    proc.wait()
    assert proc.returncode == 0

def test_test_fail(tmpdir, capsys):

    tmpdir.join('__init__.py')
    testfile = tmpdir.join('test_test_fail.py')
    testfile.write(dedent("""
        def test_foo():
            assert False
    """))

    proc = subprocess.Popen(
        ["nameko", "test", testfile.strpath], stdout=subprocess.PIPE
    )
    proc.wait()
    assert proc.returncode == 1