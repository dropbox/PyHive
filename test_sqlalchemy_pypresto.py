from sqlalchemy import func, select
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.schema import Column
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import Table
from sqlalchemy.types import String
# This import has the side effect of registering with sqlalchemy
import sqlalchemy_pypresto
import unittest


class TestSqlAlchemyPresto(unittest.TestCase):
    def test_basic_query(self):
        engine = create_engine('presto://localhost:8080/hive?schema=default')
        connection = engine.connect()
        for row in connection.execute('select count(*) as foobar from one_row'):
            self.assertEqual(row.foobar, 1)
            self.assertEqual(len(row), 1)

    def test_reflect_no_such_table(self):
        """reflecttable should throw an exception on an invalid table"""
        engine = create_engine('presto://localhost:8080/hive?schema=default')
        metadata = MetaData(bind=engine)
        self.assertRaises(NoSuchTableError,
            lambda: Table('this_does_not_exist', metadata, autoload=True))

    def test_reflect(self):
        """reflecttable should be able to fill in a table from the name"""
        engine = create_engine('presto://localhost:8080/hive?schema=default')
        metadata = MetaData(bind=engine)
        one_row = Table('one_row', metadata, autoload=True)
        self.assertEqual(len(one_row.c), 1)
        self.assertIsInstance(one_row.c.number_of_rows, Column)
        self.assertEqual(select([func.count('*')], from_obj=one_row).scalar(), 1)

    def test_reflect_include_columns(self):
        """When passed include_columns, reflecttable should filter out other columns"""
        engine = create_engine('presto://localhost:8080/hive?schema=default')
        metadata = MetaData(bind=engine)
        connection = engine.connect()
        one_row = Table('one_row', metadata)
        sqlalchemy_pypresto.dialect().reflecttable(connection, one_row, include_columns=[])
        self.assertEqual(len(one_row.c), 0)

    def test_reserved_words(self):
        """Presto uses double quotes, not backticks"""
        engine = create_engine('presto://localhost:8080/hive?schema=default')
        metadata = MetaData(bind=engine)
        # Use keywords for the table/column name
        fake_table = Table('select', metadata, Column('current_timestamp', String))
        query = str(fake_table.select(fake_table.c.current_timestamp == 'a'))
        self.assertIn('"select"', query)
        self.assertIn('"current_timestamp"', query)
        self.assertNotIn('`select`', query)
        self.assertNotIn('`current_timestamp`', query)
