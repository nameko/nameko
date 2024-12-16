#!/usr/bin/env python
import os
from codecs import open

from setuptools import find_packages, setup


here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst'), 'r', 'utf-8') as handle:
    readme = handle.read()


setup(
    name='nameko',
    version='v3.0.0-rc11',
    description='A microservices framework for Python that lets service '
                'developers concentrate on application logic and encourages '
                'testability.',
    long_description=readme,
    author='onefinestay',
    url='http://github.com/nameko/nameko',
    packages=find_packages(exclude=['test', 'test.*']),
    install_requires=[
        "click>=7.0",
        "dnspython<2 ; python_version<'3.10'",
        "eventlet>=0.20.1",
        "eventlet>=0.33.0 ; python_version>='3.10'",
        "kombu>=4.2.0",
        "kombu>=5.2.0 ; python_version>='3.10'",
        "mock>=1.2",
        "path.py>=6.2",
        "pyyaml>=5.1",
        "requests>=1.2.0",
        "six>=1.9.0",
        "werkzeug>=1.0.0,<3.0",
        "wrapt>=1.0.0",
        "packaging",
    ],
    extras_require={
        'dev': [
            "coverage==7.1.0",
            "flake8==3.9.2",
            "isort==5.11.5",
            "pylint==2.16.2",
            "pytest==6.2.5",
            "pytest-cov==2.5.1",
            "pytest-timeout==1.3.3",
            "requests==2.27.1",
            "urllib3==1.26.4",
            "websocket-client==0.48.0",
        ],
        "docs": [
            "pyenchant==3.2.2",
            "Sphinx>=5.0",
            "sphinxcontrib-spelling>=7.0",
            "sphinx-nameko-theme==0.0.3",
            "jinja2<3.1.0",  # https://github.com/readthedocs/readthedocs.org/issues/9037
        ],
        "examples": [
            "nameko-sqlalchemy==0.0.1",
            "PyJWT==2.6.0",
            "moto==4.1.2",
            "boto3==1.34.89",
            "botocore==1.34.89",  # https://github.com/boto/botocore/issues/3169
            "bcrypt==3.1.3",
            "regex==2018.2.21",
        ],
    },
    entry_points={
        "console_scripts": [
            "nameko=nameko.cli:cli",
        ],
        "pytest11": ["pytest_nameko=nameko.testing.pytest"],
    },
    zip_safe=True,
    license="Apache License, Version 2.0",
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers",
    ],
)
