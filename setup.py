#!/usr/bin/env python
from setuptools import setup, find_packages


setup(
    name='nameko',
    version='2.0.0',
    description='A microservices framework for Python that lets service '
                'developers concentrate on application logic and encourages '
                'testability.',
    author='onefinestay',
    author_email='nameko-devs@onefinestay.com',
    url='http://github.com/onefinestay/nameko',
    packages=find_packages(exclude=['test', 'test.*']),
    install_requires=[
        "eventlet>=0.15.0",
        "iso8601",
        "kombu>=3.0.1",
        "mock>=1.0.1",
        "path.py>=6.2",
        "requests>=1.2.0",
        "werkzeug>=0.9",
    ],
    extras_require={
        'dev': [
            "coverage==4.0a1",
            "flake8==2.1.0",
            "mccabe==0.3",
            "pep8==1.6.1",
            "pyflakes==0.8.1",
            "pylint==1.0.0",
            "pytest==2.4.2",
            "pytest-timeout==0.4",
            "websocket-client==0.23.0",
        ],
        'docs': [
            "pyenchant==1.6.6",
            "Sphinx==1.3",
            "sphinxcontrib-spelling==2.1.1",
            "sphinx-nameko-theme==0.0.2",
        ],
    },
    entry_points={
        'console_scripts': [
            'nameko=nameko.cli.main:main',
        ],
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
