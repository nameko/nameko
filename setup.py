#!/usr/bin/env python
import os
from codecs import open

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst'), 'r', 'utf-8') as handle:
    readme = handle.read()


setup(
    name='nameko',
    version='2.14.1',
    description='A microservices framework for Python that lets service '
                'developers concentrate on application logic and encourages '
                'testability.',
    long_description=readme,
    author='onefinestay',
    url='http://github.com/nameko/nameko',
    packages=find_packages(exclude=['test', 'test.*']),
    install_requires=[
        "dnspython<2 ; python_version<'3.10'",
        "eventlet>=0.20.1",
        "eventlet>=0.21.0 ; python_version>='3.6'",
        "eventlet>=0.25.0 ; python_version>='3.7'",
        "eventlet>=0.33.0 ; python_version>='3.10'",
        "kombu>=4.2.0",
        "kombu>=5.2.0 ; python_version>='3.10'",
        "importlib-metadata<5 ; python_version<='3.7'",  # https://github.com/celery/celery/issues/7783
        "mock>=1.2",
        "path.py>=6.2",
        "pyyaml>=5.1",
        "requests>=1.2.0",
        "six>=1.9.0",
        "werkzeug>=1.0.0",
        "wrapt>=1.0.0",
        "packaging",
    ],
    extras_require={
        'dev': [
            "coverage==5.5 ; python_version<'3'",
            "coverage==7.1.0 ; python_version>'3'",
            "flake8==3.9.2",
            "isort==4.3.21 ; python_version<'3'",
            "isort==5.11.5 ; python_version>'3'",
            "pylint==1.9.5 ; python_version<'3'",
            "pylint==2.16.2 ; python_version>'3'",
            "pytest==4.6.11 ; python_version<'3'",
            "pytest==6.2.5 ; python_version>'3'",
            "pytest-cov==2.5.1",
            "pytest-timeout==1.3.3",
            "requests==2.27.1",
            "urllib3==1.26.4",
            "websocket-client==0.48.0",
        ],
        'docs': [
            "pyenchant==1.6.11",
            "Sphinx==1.8.5",
            "sphinxcontrib-spelling==4.2.1",
            "sphinx-nameko-theme==0.0.3",
            "docutils<0.18",  # https://github.com/sphinx-doc/sphinx/issues/9788
            "jinja2<3.1.0",  # https://github.com/readthedocs/readthedocs.org/issues/9037
        ],
        'examples': [
            "nameko-sqlalchemy==0.0.1",
            "PyJWT==1.7.1 ; python_version<'3'",
            "PyJWT==2.6.0 ; python_version>'3'",
            "moto==1.3.6 ; python_version<'3'",
            "moto==4.1.2 ; python_version>'3'",
            "bcrypt==3.1.3",
            "regex==2018.2.21"
        ]
    },
    entry_points={
        'console_scripts': [
            'nameko=nameko.cli.main:main',
        ],
        'pytest11': [
            'pytest_nameko=nameko.testing.pytest'
        ]
    },
    zip_safe=True,
    license='Apache License, Version 2.0',
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers",
    ]
)
