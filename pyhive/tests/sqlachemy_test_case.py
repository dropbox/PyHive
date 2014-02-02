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
import abc


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


class SqlAlchemyTestCase(unittest.TestCase):
    __metaclass__ = abc.ABCMeta
    __test__ = False

    @with_engine_connection
    def test_basic_query(self, engine, connection):
        rows = connection.execute('SELECT * FROM one_row').fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].number_of_rows, 1)  # number_of_rows is the column name
        self.assertEqual(len(rows[0]), 1)

    @with_engine_connection
    def test_reflect_no_such_table(self, engine, connection):
        """reflecttable should throw an exception on an invalid table"""
        self.assertRaises(NoSuchTableError,
            lambda: Table('this_does_not_exist', MetaData(bind=engine), autoload=True))

    @with_engine_connection
    def test_reflect(self, engine, connection):
        """reflecttable should be able to fill in a table from the name"""
        one_row_complex = Table('one_row_complex', MetaData(bind=engine), autoload=True)
        self.assertEqual(len(one_row_complex.c), 15)
        self.assertIsInstance(one_row_complex.c.string, Column)
        self.assertEqual(select([func.count('*')], from_obj=one_row_complex).scalar(), 1)

    @with_engine_connection
    def test_reflect_include_columns(self, engine, connection):
        """When passed include_columns, reflecttable should filter out other columns"""
        one_row_complex = Table('one_row_complex', MetaData(bind=engine))
        engine.dialect.reflecttable(connection, one_row_complex, include_columns=['int'])
        self.assertEqual(len(one_row_complex.c), 1)
        self.assertIsNotNone(one_row_complex.c.int)
        self.assertRaises(AttributeError, lambda: one_row_complex.c.tinyint)
