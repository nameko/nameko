from setuptools import find_packages, setup


with open('requirements.txt', 'rb') as f:
    requirements = [i.strip().replace('==', '>=') for i in f]


setup(name='newrpc',
    version='0.1-dev',
    description='intended as an (almost) drop-in replacement for nova.rpc'
        ' with no dependency on nova.',
    author='Edward George',
    author_email='rqjneqtrbetr@tznvy.pbz'.decode('rot13'),
    packages=['newrpc', ],
    install_requires=requirements,
    test_requires=['pytest==2.2.4', 'mock==1.0b1', ],
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 2.7",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers", ])
