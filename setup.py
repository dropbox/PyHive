#!/usr/bin/env python

from setuptools import setup
import pyhive

with open('README.rst') as readme:
    long_description = readme.read()

setup(
    name="PyHive",
    version=pyhive.__version__,
    description="Python interface to Hive",
    long_description=long_description,
    url='https://github.com/jingw/PyHive',
    author="Jing Wang",
    author_email="jing@dropbox.com",
    packages=['pyhive'],
    classifiers=[
        "Topic :: Database",
    ],
    extras_require={
        "Presto": ['requests'],
        "Hive": ['sasl>=0.1.3', 'thrift>=0.9.1'],
        "SQLAlchemy": ['sqlalchemy==0.5.8'],
    },
    test_suite='nose.collector',
    tests_require=[
        'mock',
        'nose',
        'requests',
        'sasl>=0.1.3',
        'sqlalchemy==0.5.8',
        'thrift>=0.9.1',
    ],
    package_data={
        '': ['*.rst'],
    },
)
