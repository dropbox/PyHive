======
PyHive
======

PyHive is a collection of Python `DB-API <http://www.python.org/dev/peps/pep-0249/>`_ and
`SQLAlchemy <http://www.sqlalchemy.org/>`_ interfaces for `Presto <http://prestodb.io/>`_ and
`Hive <http://hive.apache.org/>`_.

Usage
=====

DB-API
------
.. code-block:: python

    from pyhive import presto
    cursor = presto.connect('localhost').cursor()
    cursor.execute('SELECT * FROM my_awesome_data LIMIT 10')
    print cursor.fetchone()
    print cursor.fetchall()

SQLAlchemy
----------
First install this package to register it with SQLAlchemy (see ``setup.py``).

.. code-block:: python

    from sqlalchemy import *
    from sqlalchemy.engine import create_engine
    from sqlalchemy.schema import *
    engine = create_engine('presto://localhost:8080/hive/default')
    logs = Table('my_awesome_data', MetaData(bind=engine), autoload=True)
    print select([func.count('*')], from_obj=logs).scalar()

Note: query generation functionality is not exhaustive or fully tested, but there should be no
problem with raw SQL.

Passing session configuration
-----------------------------

.. code-block:: python

    # DB-API
    hive.connect('localhost', configuration={'hive.exec.reducers.max': '123'})
    presto.connect('localhost', session_props={'query_max_run_time': '1234m'})
    # SQLAlchemy
    create_engine(
        'hive://user@host:10000/database',
        connect_args={'configuration': {'hive.exec.reducers.max': '123'}},
    )

Requirements
============

Install using

- ``pip install pyhive[hive]`` for the Hive interface and
- ``pip install pyhive[presto]`` for the Presto interface.

`PyHive` works with

- Python 2.7
- For Presto: Presto install
- For Hive: `HiveServer2 <https://cwiki.apache.org/confluence/display/Hive/Setting+up+HiveServer2>`_ daemon

There's also a `third party Conda package <https://binstar.org/blaze/pyhive>`_.

Testing
=======
.. image:: https://travis-ci.org/dropbox/PyHive.svg
    :target: https://travis-ci.org/dropbox/PyHive
.. image:: http://codecov.io/github/dropbox/PyHive/coverage.svg?branch=master
    :target: http://codecov.io/github/dropbox/PyHive?branch=master

Run the following in an environment with Hive/Presto::

    ./scripts/make_test_tables.sh
    virtualenv --no-site-packages env
    source env/bin/activate
    pip install -e .
    pip install -r dev_requirements.txt
    py.test

WARNING: This drops/creates tables named ``one_row``, ``one_row_complex``, and ``many_rows``, plus a
database called ``pyhive_test_database``.
