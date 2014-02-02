PyHive
======

PyHive is a collection of Python `DB-API <http://www.python.org/dev/peps/pep-0249/>`_ and
`SQLAlchemy <http://www.sqlalchemy.org/>`_ interfaces for `Presto <http://prestodb.io/>`_ and
`Hive <http://hive.apache.org/>`_.

Usage
=====

DB-API
-----
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

Requirements
============

- Presto DB-API: Just a Presto install
- Hive DB-API: HiveServer2 daemon, ``TCLIService`` (from Hive), ``thrift_sasl`` (from `Cloudera
  <https://github.com/y-lan/python-hiveserver2/blob/master/src/cloudera/thrift_sasl.py>`_)
- SQLAlchemy integration: ``sqlalchemy``

Testing
=======

Run the following in an environment with Hive/Presto::

    ./scripts/make_test_tables.sh
    python setup.py test

WARNING: This drops/creates tables named ``one_row``, ``one_row_complex``, and ``many_rows``.
