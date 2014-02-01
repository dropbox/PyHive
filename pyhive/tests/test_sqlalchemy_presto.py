# This import has the side effect of registering with sqlalchemy
from pyhive import sqlalchemy_presto
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.schema import Column
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import Table
from sqlalchemy.types import String
import contextlib
import functools
import unittest


def with_engine_connection(fn):
    """Pass a connection to the given function and handle cleanup.

    The connection is taken from ``self.create_engine()``.
    """
    @functools.wraps(fn)
    def wrapped_fn(self, *args, **kwargs):
        engine = self.create_engine()
        try:
            with contextlib.closing(engine.connect()) as connection:
                fn(self, engine, connection, *args, **kwargs)
        finally:
            engine.dispose()
    return wrapped_fn


class TestSqlAlchemyPresto(unittest.TestCase):
    def create_engine(self):
        return create_engine('presto://localhost:8080/hive/default')

    def test_url_default(self):
        engine = create_engine('presto://localhost:8080/hive')
        try:
            with contextlib.closing(engine.connect()) as connection:
                self.assertEqual(connection.execute('SELECT 1 AS foobar FROM one_row').scalar(), 1)
        finally:
            engine.dispose()

    @with_engine_connection
    def test_basic_query(self, engine, connection):
        for row in connection.execute('select count(*) as foobar from one_row'):
            self.assertEqual(row.foobar, 1)
            self.assertEqual(len(row), 1)

    @with_engine_connection
    def test_reflect_no_such_table(self, engine, connection):
        """reflecttable should throw an exception on an invalid table"""
        metadata = MetaData(bind=engine)
        self.assertRaises(NoSuchTableError,
            lambda: Table('this_does_not_exist', metadata, autoload=True))

    @with_engine_connection
    def test_reflect(self, engine, connection):
        """reflecttable should be able to fill in a table from the name"""
        metadata = MetaData(bind=engine)
        one_row = Table('one_row', metadata, autoload=True)
        self.assertEqual(len(one_row.c), 1)
        self.assertIsInstance(one_row.c.number_of_rows, Column)
        self.assertEqual(select([func.count('*')], from_obj=one_row).scalar(), 1)

    @with_engine_connection
    def test_reflect_include_columns(self, engine, connection):
        """When passed include_columns, reflecttable should filter out other columns"""
        metadata = MetaData(bind=engine)
        one_row = Table('one_row_complex', metadata)
        sqlalchemy_presto.dialect().reflecttable(connection, one_row, include_columns=['a'])
        self.assertEqual(len(one_row.c), 1)
        self.assertIsNotNone(one_row.c.a)
        self.assertRaises(AttributeError, lambda: one_row.c.b)

    @with_engine_connection
    def test_reserved_words(self, engine, connection):
        """Presto uses double quotes, not backticks"""
        metadata = MetaData(bind=engine)
        # Use keywords for the table/column name
        fake_table = Table('select', metadata, Column('current_timestamp', String))
        query = str(fake_table.select(fake_table.c.current_timestamp == 'a'))
        self.assertIn('"select"', query)
        self.assertIn('"current_timestamp"', query)
        self.assertNotIn('`select`', query)
        self.assertNotIn('`current_timestamp`', query)
