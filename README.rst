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

Requirements
============

- Python 2.7
- For Presto: Just a Presto install
- For Hive

  - `HiveServer2 <https://cwiki.apache.org/confluence/display/Hive/Setting+up+HiveServer2>`_ daemon
  - ``TCLIService`` (from Hive in ``/usr/lib/hive/lib/py``)
  - ``thrift_sasl`` (from `Cloudera <https://github.com/y-lan/python-hiveserver2/blob/master/src/cloudera/thrift_sasl.py>`_)

Testing
=======

Run the following in an environment with Hive/Presto::

    ./scripts/make_test_tables.sh
    virtualenv --no-site-packages env
    source env/bin/activate
    pip install -r dev_requirements.txt
    py.test

WARNING: This drops/creates tables named ``one_row``, ``one_row_complex``, and ``many_rows``, plus a
database called ``pyhive_test_database``.
