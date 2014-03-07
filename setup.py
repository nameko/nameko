#!/usr/bin/env python
import imp
import os
from setuptools import setup, find_packages
from os.path import abspath, dirname, join


setup_dir = dirname(abspath(__file__))


CODE_DIRECTORY = 'nameko'


# Import metadata. Normally this would just be:
#
#     from nameko import metadata
#
# However, when we do this, we also import `nameko/__init__.py'. If this
# imports names from some other modules and these modules have third-party
# dependencies that need installing (which happens after this file is run), the
# script will crash. What we do instead is to load the metadata module by path
# instead, effectively side-stepping the dependency problem. Please make sure
# metadata has no dependencies, otherwise they will need to be added to
# the setup_requires keyword.
metadata = imp.load_source(
    'metadata', os.path.join(CODE_DIRECTORY, 'metadata.py'))


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


def read(filename):
    """Return the contents of a file.

    :param filename: file path
    :type filename: :class:`str`
    :return: the file's content
    :rtype: :class:`str`
    """
    with open(os.path.join(os.path.dirname(__file__), filename)) as f:
        return f.read()


setup(
    name=metadata.package,
    version=metadata.version,
    description=metadata.description,
    author=metadata.authors[0],
    author_email=metadata.emails[0],
    maintainer=metadata.authors[0],
    maintainer_email=metadata.emails[0],
    url=metadata.url,
    long_description=read('readme.rst'),
    packages=find_packages(exclude=['test', 'test.*']),
    install_requires=requirements,
    dependency_links=dependency_links,
    zip_safe=True,
    license='Apache License, Version 2.0',
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 2.7",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers",
    ],
    entry_points={
        'console_scripts': [
            'nameko_doc = nameko.nameko_doc.main:entry_point'
        ],
    },
)
