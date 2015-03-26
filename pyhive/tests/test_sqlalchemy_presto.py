from __future__ import absolute_import
from __future__ import unicode_literals
from pyhive.tests.sqlachemy_test_case import SqlAlchemyTestCase
from pyhive.tests.sqlachemy_test_case import with_engine_connection
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Column
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import Table
from sqlalchemy.types import String
import contextlib
import unittest


class TestSqlAlchemyPresto(unittest.TestCase, SqlAlchemyTestCase):
    def create_engine(self):
        return create_engine('presto://localhost:8080/hive/default?source={}'.format(self.id()))

    def test_bad_format(self):
        self.assertRaises(
            ValueError,
            lambda: create_engine('presto://localhost:8080/hive/default/what'),
        )

    @with_engine_connection
    def test_reflect_select(self, engine, connection):
        """reflecttable should be able to fill in a table from the name"""
        one_row_complex = Table('one_row_complex', MetaData(bind=engine), autoload=True)
        # Presto ignores the union and decimal columns
        self.assertEqual(len(one_row_complex.c), 15 - 2)
        self.assertIsInstance(one_row_complex.c.string, Column)
        rows = one_row_complex.select().execute().fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(list(rows[0]), [
            True,
            127,
            32767,
            2147483647,
            9223372036854775807,
            0.5,
            0.25,
            'a string',
            '1970-01-01 00:00:00.000',
            '123',
            [1, 2],
            {"1": 2, "3": 4},  # Presto converts all keys to strings so that they're valid JSON
            [1, 2],  # struct is returned as a list of elements
            #'{0:1}',
            #0.1,
        ])

    def test_url_default(self):
        engine = create_engine('presto://localhost:8080/hive')
        try:
            with contextlib.closing(engine.connect()) as connection:
                self.assertEqual(connection.execute('SELECT 1 AS foobar FROM one_row').scalar(), 1)
        finally:
            engine.dispose()

    @with_engine_connection
    def test_reserved_words(self, engine, connection):
        """Presto uses double quotes, not backticks"""
        # Use keywords for the table/column name
        fake_table = Table('select', MetaData(bind=engine), Column('current_timestamp', String))
        query = str(fake_table.select(fake_table.c.current_timestamp == 'a'))
        self.assertIn('"select"', query)
        self.assertIn('"current_timestamp"', query)
        self.assertNotIn('`select`', query)
        self.assertNotIn('`current_timestamp`', query)
