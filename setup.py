#!/usr/bin/env python
# enconding: utf-8
import sys
import os
from setuptools import setup, find_packages

from equeue import __version__

BASE_PATH = os.path.dirname(__file__)
setup(
    name='equeue',
    version=__version__,
    description='Elastic Queue is a library to bear a PubSub projects. ',
    long_description=open(os.path.join(BASE_PATH, 'README.md')).read(),
    author='Jesue Junior',
    author_email='jesuesousa@gmail.com',
    url='https://github.com/jesuejunior/equeue',
    packages=find_packages(),
    install_requires=['amqp==2.1.0', 'simplejson>=3.8.2', 'six==1.10.0'],
    test_suite='tests',
    tests_require=['tox>=2.3.1'] + (
        ['mock==1.3.0'] if sys.version_info.major == 2 else []
    ),
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
