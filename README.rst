PyHive
======

PyHive is a collection of python DBAPI and SQLAlchemy wrappers for Presto's REST interface and
HiveServer2's Thrift interface.

Usage
=====

DBAPI::

    cursor = presto.connect('localhost').cursor()
    cursor.execute('SELECT * FROM user LIMIT 10')
    print cursor.fetchone()
    print cursor.fetchall()

SQLAlchemy::

    engine = create_engine('presto://localhost:8080/hive?schema=default')
    metadata = MetaData(bind=engine)
    user = Table('user', metadata, autoload=True)
    print select([func.count('*')], from_obj=user).scalar()

Requirements
============

- Presto DBAPI: Just a Presto install
- Hive DBAPI: HiveServer2 daemon, `TCLIService`, `thrift`, `sasl`, `thrift_sasl`
- SQLAlchemy integration: `sqlalchemy` version 0.5
