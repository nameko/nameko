#!/usr/bin/env python
import os
from codecs import open

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst'), 'r', 'utf-8') as handle:
    readme = handle.read()


setup(
    name='nameko',
    version='2.8.1',
    description='A microservices framework for Python that lets service '
                'developers concentrate on application logic and encourages '
                'testability.',
    long_description=readme,
    author='onefinestay',
    url='http://github.com/nameko/nameko',
    packages=find_packages(exclude=['test', 'test.*']),
    install_requires=[
        "eventlet>=0.16.1",
        "kombu>=3.0.1,<4",
        "mock>=1.2",
        "path.py>=6.2",
        "pyyaml>=3.10",
        "requests>=1.2.0",
        "six>=1.9.0",
        "werkzeug>=0.9",
        "wrapt>=1.0.0",
    ],
    extras_require={
        'dev': [
            "coverage==4.4.1",
            "flake8==3.3.0",
            "isort==4.2.15",
            "mccabe==0.6.1",
            "pycodestyle==2.3.1",
            "pyflakes==1.5.0",
            "pylint==1.7.1",
            "pytest==2.7.3",
            "pytest-cov==2.1.0",
            "pytest-timeout==0.4",
            "requests==2.17.3",
            "urllib3==1.21.1",
            "websocket-client==0.23.0",
        ],
        'docs': [
            "pyenchant==1.6.6",
            "Sphinx==1.3",
            "sphinxcontrib-spelling==2.1.1",
            "sphinx-nameko-theme==0.0.3",
        ],
        'examples': [
            "nameko-sqlalchemy==0.0.1",
            "PyJWT==1.5.2",
            "moto==1.0.1",
            "bcrypt==3.1.3"
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
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers",
    ]
)
