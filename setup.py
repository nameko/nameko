from setuptools import setup
from setuptools.command.test import test as TestCommand
from os.path import dirname, join
import sys


setup_dir = dirname(__file__) or '.'


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = [
            '--cov', 'nameko',
            '--junitxml=test-results.xml',
            join(setup_dir, 'test'),
        ]
        self.test_suite = True

    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)


def parse_requirments(fn, dependency_links):
    requirements = []
    with open(fn, 'rb') as f:
        for dep in f:
            dep = dep.strip()
            # need to make test_requirements.txt work with
            # setuptools like it would work with `pip -r`
            # URLs will not work, so we transform them to
            # dependency_links and requirements
            if dep.startswith('git+'):
                dependency_links.append(dep)
                _, dep = dep.rsplit('#egg=', 1)
                dep = dep.replace('-', '==', 1)
            requirements.append(dep)

    return requirements, dependency_links

requirements, dependency_links = parse_requirments(
    join(setup_dir, 'requirements.txt'), [])

test_requirements, dependency_links = parse_requirments(
    join(setup_dir, 'test_requirements.txt'),
    dependency_links)

setup(
    name='nameko',
    version='0.1-dev',
    description='service framework supporting multiple'
                'messaging and RPC implementations',
    author='onefinestay',
    author_email='engineering@onefinestay.com',
    url='http://github.com/onefinestay/nameko',
    packages=['nameko', ],
    package_dir={'': setup_dir},
    install_requires=requirements,
    tests_require=test_requirements,
    dependency_links=dependency_links,
    cmdclass={'test': PyTest},
    license='Apache License, Version 2.0',
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 2.7",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers", ]
)
