from __future__ import absolute_import
from __future__ import unicode_literals
from pyhive.sqlalchemy_hive import HiveDate
from pyhive.sqlalchemy_hive import HiveDecimal
from pyhive.sqlalchemy_hive import HiveTimestamp
from pyhive.tests.sqlachemy_test_case import SqlAlchemyTestCase
from pyhive.tests.sqlachemy_test_case import with_engine_connection
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Column
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import Table
from sqlalchemy.types import String, Integer
import contextlib
import datetime
import decimal
import sqlalchemy.types
import unittest

_ONE_ROW_COMPLEX_CONTENTS = [
    True,
    127,
    32767,
    2147483647,
    9223372036854775807,
    0.5,
    0.25,
    'a string',
    datetime.datetime(1970, 1, 1),
    '123',
    '[1,2]',
    '{1:2,3:4}',
    '{"a":1,"b":2}',
    '{0:1}',
    decimal.Decimal('0.1'),
]


class TestSqlAlchemyHive(unittest.TestCase, SqlAlchemyTestCase):
    def create_engine(self):
        return create_engine('hive://localhost:10000/default')

    @with_engine_connection
    def test_reflect_select(self, engine, connection):
        """reflecttable should be able to fill in a table from the name"""
        one_row_complex = Table('one_row_complex', MetaData(bind=engine), autoload=True)
        self.assertEqual(len(one_row_complex.c), 15)
        self.assertIsInstance(one_row_complex.c.string, Column)
        rows = one_row_complex.select().execute().fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(list(rows[0]), _ONE_ROW_COMPLEX_CONTENTS)

    @with_engine_connection
    def test_type_map(self, engine, connection):
        """sqlalchemy should use the dbapi_type_map to infer types from raw queries"""
        rows = connection.execute('SELECT * FROM one_row_complex').fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(list(rows[0]), _ONE_ROW_COMPLEX_CONTENTS)

    @with_engine_connection
    def test_reserved_words(self, engine, connection):
        """Hive uses backticks"""
        # Use keywords for the table/column name
        fake_table = Table('select', MetaData(bind=engine), Column('map', sqlalchemy.types.String))
        query = str(fake_table.select(fake_table.c.map == 'a'))
        self.assertIn('`select`', query)
        self.assertIn('`map`', query)
        self.assertNotIn('"select"', query)
        self.assertNotIn('"map"', query)

    def test_switch_database(self):
        engine = create_engine('hive://localhost:10000/pyhive_test_database')
        try:
            with contextlib.closing(engine.connect()) as connection:
                self.assertEqual(
                    connection.execute('SHOW TABLES').fetchall(),
                    [('dummy_table',)]
                )
                connection.execute('USE default')
                self.assertIn(
                    ('one_row',),
                    connection.execute('SHOW TABLES').fetchall()
                )
        finally:
            engine.dispose()

    @with_engine_connection
    def test_insert(self, engine, connection):
        tb = Table('insert_test', MetaData(bind=engine), Column('a', Integer))

        with engine.connect() as conn:
            conn.execute(tb.insert(), [{'a': 10000}])

        with engine.connect() as conn:
            result = conn.execute(tb.select())

        expected = [(x,) for x in list(range(10001))]
        self.assertEqual(result, expected)

    def test_lots_of_types(self, engine, connection):
        # Presto doesn't have raw CREATE TABLE support, so we ony test hive
        # take type list from sqlalchemy.types
        types = [
            'INT', 'CHAR', 'VARCHAR', 'NCHAR', 'TEXT', 'Text', 'FLOAT',
            'NUMERIC', 'DECIMAL', 'TIMESTAMP', 'DATETIME', 'CLOB', 'BLOB',
            'BOOLEAN', 'SMALLINT', 'DATE', 'TIME',
            'String', 'Integer', 'SmallInteger',
            'Numeric', 'Float', 'DateTime', 'Date', 'Time', 'Binary',
            'Boolean', 'Unicode', 'UnicodeText',
        ]
        cols = []
        for i, t in enumerate(types):
            cols.append(Column(str(i), getattr(sqlalchemy.types, t)))
        cols.append(Column('hive_date', HiveDate))
        cols.append(Column('hive_decimal', HiveDecimal))
        cols.append(Column('hive_timestamp', HiveTimestamp))
        table = Table('test_table', MetaData(bind=engine), *cols, schema='pyhive_test_database')
        table.drop(checkfirst=True)
        table.create()
        connection.execute('SET mapred.job.tracker=local')
        connection.execute('USE pyhive_test_database')
        connection.execute("""
        INSERT OVERWRITE TABLE test_table
        SELECT
            1, "a", "a", "a", "a", "a", 0.1,
            0.1, 0.1, 0, 0, "a", "a",
            false, 1, 0, 0,
            "a", 1, 1,
            0.1, 0.1, 0, 0, 0, "a",
            false, "a", "a",
            0, 0.1, 123 + 2000
        FROM default.one_row
        """)
        row = connection.execute(table.select()).fetchone()
        self.assertEqual(row.hive_date, datetime.date(1970, 1, 1))
        self.assertEqual(row.hive_decimal, decimal.Decimal('0.1'),)
        self.assertEqual(row.hive_timestamp, datetime.datetime(1970, 1, 1, 0, 0, 2, 123))
        table.drop()
