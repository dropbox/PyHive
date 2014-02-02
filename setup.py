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
        "Presto": ['requests>=1.0.0'],
        "Hive": ['sasl>=0.1.3', 'thrift>=0.8.0'],
        "SQLAlchemy": ['sqlalchemy>=0.5.0'],
    },
    test_suite='nose.collector',
    tests_require=[
        'mock>=1.0.0',
        'nose',
        'requests>=1.0.0',
        'sasl>=0.1.3',
        'sqlalchemy>=0.5.0',
        'thrift>=0.8.0',
    ],
    package_data={
        '': ['*.rst'],
    },
    entry_points={
        # New versions
        'sqlalchemy.dialects': [
            'hive = pyhive.sqlalchemy_hive:HiveDialect',
            'presto = pyhive.sqlalchemy_presto:PrestoDialect',
        ],
        # Version 0.5
        'sqlalchemy.databases': [
            'hive = pyhive.sqlalchemy_hive:HiveDialect',
            'presto = pyhive.sqlalchemy_presto:PrestoDialect',
        ],
    }
)
