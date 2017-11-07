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

    from pyhive import presto  # or import hive
    cursor = presto.connect('localhost').cursor()
    cursor.execute('SELECT * FROM my_awesome_data LIMIT 10')
    print cursor.fetchone()
    print cursor.fetchall()

DB-API (asynchronous)
---------------------
.. code-block:: python

    from pyhive import hive
    from TCLIService.ttypes import TOperationState
    cursor = hive.connect('localhost').cursor()
    cursor.execute('SELECT * FROM my_awesome_data LIMIT 10', async=True)

    status = cursor.poll().operationState
    while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
        logs = cursor.fetch_logs()
        for message in logs:
            print message

        # If needed, an asynchronous query can be cancelled at any time with:
        # cursor.cancel()

        status = cursor.poll().operationState

    print cursor.fetchall()

SQLAlchemy
----------
First install this package to register it with SQLAlchemy (see ``setup.py``).

.. code-block:: python

    from sqlalchemy import *
    from sqlalchemy.engine import create_engine
    from sqlalchemy.schema import *
    # Presto
    engine = create_engine('presto://localhost:8080/hive/default')
    # Hive
    engine = create_engine('hive://localhost:10000/default')
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
        'presto://user@host:443/hive',
        connect_args={'protocol': 'https',
                      'session_props': {'query_max_run_time': '1234m'}}
    )
    create_engine(
        'hive://user@host:10000/database',
        connect_args={'configuration': {'hive.exec.reducers.max': '123'}},
    )
    # SQLAlchemy with LDAP
    create_engine(
        'hive://user:password@host:10000/database',
        connect_args={'auth': 'LDAP'},
    )

Requirements
============

Install using

- ``pip install pyhive[hive]`` for the Hive interface and
- ``pip install pyhive[presto]`` for the Presto interface.

PyHive works with

- Python 2.7 / Python 3
- For Presto: Presto install
- For Hive: `HiveServer2 <https://cwiki.apache.org/confluence/display/Hive/Setting+up+HiveServer2>`_ daemon
- For Python 3 + Hive + SASL, you currently need to install an unreleased version of ``thrift_sasl``
  (``pip install git+https://github.com/cloudera/thrift_sasl``).
  At the time of writing, the latest version of ``thrift_sasl`` was 0.2.1.

Changelog
=========
See https://github.com/dropbox/PyHive/releases.

Contributing
============
- Please fill out the Dropbox Contributor License Agreement at https://opensource.dropbox.com/cla/ and note this in your pull request.
- Changes must come with tests, with the exception of trivial things like fixing comments. See .travis.yml for the test environment setup.
- Notes on project scope:

  - This project is intended to be a minimal Hive/Presto client that does that one thing and nothing else.
    Features that can be implemented on top of PyHive, such integration with your favorite data analysis library, are likely out of scope.
  - We prefer having a small number of generic features over a large number of specialized, inflexible features.
    For example, the Presto code takes an arbitrary ``requests_session`` argument for customizing HTTP calls, as opposed to having a separate parameter/branch for each ``requests`` option.

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
