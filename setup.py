from setuptools import setup


with open('requirements.txt', 'rb') as f:
    requirements = [i.strip() for i in f]


setup(name='nameko',
    version='0.1.1',
    description='service framework supporting multiple'
                'messaging and RPC implementations',
    author='onefinestay',
    author_email='engineering@onefinestay.com',
    packages=['nameko', ],
    install_requires=requirements,
    test_requires=['pytest>=2.2.4', 'mock>=1.0b1', ],
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 2.7",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers", ])
