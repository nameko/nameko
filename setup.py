#!/usr/bin/env python
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
from os.path import abspath, dirname, join
import sys


setup_dir = dirname(abspath(__file__))


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = [
            '--cov', 'nameko',
            join(setup_dir, 'test'),
        ]
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)


def parse_requirements(fn, dependency_links):
    requirements = []
    with open(fn, 'rb') as f:
        for dep in f:
            dep = dep.strip()
            # need to make test_requirements.txt work with
            # setuptools like it would work with `pip -r`
            # URLs will not work, so we transform them to
            # dependency_links and requirements
            if dep.startswith('-e '):
                dep = dep[3:]

            if dep.startswith('git+'):
                dependency_links.append(dep)
                _, dep = dep.rsplit('#egg=', 1)
                dep = dep.replace('-', '==', 1)
            requirements.append(dep)

    return requirements, dependency_links

requirements, dependency_links = parse_requirements(
    join(setup_dir, 'requirements.txt'), [])

contrib_requirements, dependency_links = parse_requirements(
    join(setup_dir, 'contrib_requirements.txt'),
    dependency_links)

test_requirements, dependency_links = parse_requirements(
    join(setup_dir, 'test_requirements.txt'),
    dependency_links)

setup(
    name='nameko',
    version='1.0.1',
    description='service framework supporting multiple'
                'messaging and RPC implementations',
    author='onefinestay',
    author_email='engineering@onefinestay.com',
    url='http://github.com/onefinestay/nameko',
    packages=find_packages(exclude=['test', 'test.*']),
    install_requires=requirements + contrib_requirements,
    tests_require=test_requirements,
    dependency_links=dependency_links,
    zip_safe=True,
    cmdclass={'test': PyTest},
    license='Apache License, Version 2.0',
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 2.7",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers",
    ]
)
