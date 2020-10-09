#!/usr/bin/env python
import os
from codecs import open

from setuptools import find_packages, setup


here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst'), 'r', 'utf-8') as handle:
    readme = handle.read()


setup(
    name='nameko',
    version='v3.0.0-rc9',
    description='A microservices framework for Python that lets service '
                'developers concentrate on application logic and encourages '
                'testability.',
    long_description=readme,
    author='onefinestay',
    url='http://github.com/nameko/nameko',
    packages=find_packages(exclude=['test', 'test.*']),
    install_requires=[
        "click>=7.0",
        "dnspython<2",
        "eventlet>=0.20.1;python_version<'3.6'",
        "eventlet>=0.21.0;python_version>='3.6' and python_version<'3.7'",
        "eventlet>=0.26.0;python_version>='3.7'",
        "kombu>=4.2.0,<5",
        "mock>=1.2",
        "path.py>=6.2",
        "pyyaml>=5.1",
        "requests>=1.2.0",
        "six>=1.9.0",
        "werkzeug>=0.9",
        "wrapt>=1.0.0",
    ],
    extras_require={
        'dev': [
            "astroid==2.2.5;python_version>='3'",
            "astroid==1.6.5;python_version<'3'",
            "coverage==4.5.1",
            "flake8==3.3.0",
            "isort==4.2.15",
            "mccabe==0.6.1",
            "pycodestyle==2.3.1",
            "pyflakes==1.5.0",
            "pylint==2.3.1;python_version>='3'",
            "pylint==1.8.0;python_version<'3'",
            "pytest==4.3.1",
            "pytest-cov==2.5.1",
            "pytest-timeout==1.3.3",
            "requests==2.19.1",
            "urllib3==1.23",
            "websocket-client==0.48.0",
        ],
        'docs': [
            "pyenchant==1.6.11",
            "Sphinx==1.8.5",
            "sphinxcontrib-spelling==4.2.1",
            "sphinx-nameko-theme==0.0.3",
        ],
        'examples': [
            "nameko-sqlalchemy==0.0.1",
            "PyJWT==1.5.2",
            "moto==1.3.6",
            "bcrypt==3.1.3",
            "regex==2018.2.21"
        ]
    },
    entry_points={
        'console_scripts': [
            'nameko=nameko.cli:cli',
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
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers",
    ]
)
