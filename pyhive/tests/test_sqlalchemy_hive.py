from __future__ import absolute_import
from __future__ import unicode_literals
from builtins import str
from pyhive.sqlalchemy_hive import HiveDate
from pyhive.sqlalchemy_hive import HiveDecimal
from pyhive.sqlalchemy_hive import HiveTimestamp
from pyhive.tests.sqlalchemy_test_case import SqlAlchemyTestCase
from pyhive.tests.sqlalchemy_test_case import with_engine_connection
from sqlalchemy import types
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Column
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import Table
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
    b'123',
    '[1,2]',
    '{1:2,3:4}',
    '{"a":1,"b":2}',
    '{0:1}',
    decimal.Decimal('0.1'),
]


# [
# ('boolean', 'boolean', ''),
# ('tinyint', 'tinyint', ''),
# ('smallint', 'smallint', ''),
# ('int', 'int', ''),
# ('bigint', 'bigint', ''),
# ('float', 'float', ''),
# ('double', 'double', ''),
# ('string', 'string', ''),
# ('timestamp', 'timestamp', ''),
# ('binary', 'binary', ''),
# ('array', 'array<int>', ''),
# ('map', 'map<int,int>', ''),
# ('struct', 'struct<a:int,b:int>', ''),
# ('union', 'uniontype<int,string>', ''),
# ('decimal', 'decimal(10,1)', '')
# ]


class TestSqlAlchemyHive(unittest.TestCase, SqlAlchemyTestCase):
    def create_engine(self):
        return create_engine('hive://localhost:10000/default')

    @with_engine_connection
    def test_dotted_column_names(self, engine, connection):
        """When Hive returns a dotted column name, both the non-dotted version should be available
        as an attribute, and the dotted version should remain available as a key.
        """
        row = connection.execute('SELECT * FROM one_row').fetchone()
        assert row.keys() == ['number_of_rows']
        assert 'number_of_rows' in row
        assert row.number_of_rows == 1
        assert row['number_of_rows'] == 1
        assert getattr(row, 'one_row.number_of_rows') == 1
        assert row['one_row.number_of_rows'] == 1

    @with_engine_connection
    def test_dotted_column_names_raw(self, engine, connection):
        """When Hive returns a dotted column name, and raw mode is on, nothing should be modified.
        """
        row = connection.execution_options(hive_raw_colnames=True) \
            .execute('SELECT * FROM one_row').fetchone()
        assert row.keys() == ['one_row.number_of_rows']
        assert 'number_of_rows' not in row
        assert getattr(row, 'one_row.number_of_rows') == 1
        assert row['one_row.number_of_rows'] == 1

    @with_engine_connection
    def test_reflect_select(self, engine, connection):
        """reflecttable should be able to fill in a table from the name"""
        one_row_complex = Table('one_row_complex', MetaData(bind=engine), autoload=True)
        self.assertEqual(len(one_row_complex.c), 15)
        self.assertIsInstance(one_row_complex.c.string, Column)
        row = one_row_complex.select().execute().fetchone()
        self.assertEqual(list(row), _ONE_ROW_COMPLEX_CONTENTS)

        # TODO some of these types could be filled in better
        self.assertIsInstance(one_row_complex.c.boolean.type, types.Boolean)
        self.assertIsInstance(one_row_complex.c.tinyint.type, types.Integer)
        self.assertIsInstance(one_row_complex.c.smallint.type, types.Integer)
        self.assertIsInstance(one_row_complex.c.int.type, types.Integer)
        self.assertIsInstance(one_row_complex.c.bigint.type, types.BigInteger)
        self.assertIsInstance(one_row_complex.c.float.type, types.Float)
        self.assertIsInstance(one_row_complex.c.double.type, types.Float)
        self.assertIsInstance(one_row_complex.c.string.type, types.String)
        self.assertIsInstance(one_row_complex.c.timestamp.type, HiveTimestamp)
        self.assertIsInstance(one_row_complex.c.binary.type, types.String)
        self.assertIsInstance(one_row_complex.c.array.type, types.String)
        self.assertIsInstance(one_row_complex.c.map.type, types.String)
        self.assertIsInstance(one_row_complex.c.struct.type, types.String)
        self.assertIsInstance(one_row_complex.c.union.type, types.String)
        self.assertIsInstance(one_row_complex.c.decimal.type, HiveDecimal)

    @with_engine_connection
    def test_type_map(self, engine, connection):
        """sqlalchemy should use the dbapi_type_map to infer types from raw queries"""
        row = connection.execute('SELECT * FROM one_row_complex').fetchone()
        self.assertListEqual(list(row), _ONE_ROW_COMPLEX_CONTENTS)

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
                self.assertIn(
                    ('dummy_table',),
                    connection.execute('SHOW TABLES').fetchall()
                )
                connection.execute('USE default')
                self.assertIn(
                    ('one_row',),
                    connection.execute('SHOW TABLES').fetchall()
                )
        finally:
            engine.dispose()

    @with_engine_connection
    def test_lots_of_types(self, engine, connection):
        # Presto doesn't have raw CREATE TABLE support, so we ony test hive
        # take type list from sqlalchemy.types
        types = [
            'INT', 'CHAR', 'VARCHAR', 'NCHAR', 'TEXT', 'Text', 'FLOAT',
            'NUMERIC', 'DECIMAL', 'TIMESTAMP', 'DATETIME', 'CLOB', 'BLOB',
            'BOOLEAN', 'SMALLINT', 'DATE', 'TIME',
            'String', 'Integer', 'SmallInteger',
            'Numeric', 'Float', 'DateTime', 'Date', 'Time', 'LargeBinary',
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
        big_number = 10 ** 10 - 1
        connection.execute("""
        INSERT OVERWRITE TABLE test_table
        SELECT
            1, "a", "a", "a", "a", "a", 0.1,
            0.1, 0.1, 0, 0, "a", "a",
            false, 1, 0, 0,
            "a", 1, 1,
            0.1, 0.1, 0, 0, 0, "a",
            false, "a", "a",
            0, %d, 123 + 2000
        FROM default.one_row
        """, big_number)
        row = connection.execute(table.select()).fetchone()
        self.assertEqual(row.hive_date, datetime.date(1970, 1, 1))
        self.assertEqual(row.hive_decimal, decimal.Decimal(big_number))
        self.assertEqual(row.hive_timestamp, datetime.datetime(1970, 1, 1, 0, 0, 2, 123000))
        table.drop()

    @with_engine_connection
    def test_insert_select(self, engine, connection):
        one_row = Table('one_row', MetaData(bind=engine), autoload=True)
        table = Table('insert_test', MetaData(bind=engine),
                      Column('a', sqlalchemy.types.Integer),
                      schema='pyhive_test_database')
        table.drop(checkfirst=True)
        table.create()
        connection.execute('SET mapred.job.tracker=local')
        # NOTE(jing) I'm stuck on a version of Hive without INSERT ... VALUES
        connection.execute(table.insert().from_select(['a'], one_row.select()))

        result = table.select().execute().fetchall()
        expected = [(1,)]
        self.assertEqual(result, expected)

    @with_engine_connection
    def test_insert_values(self, engine, connection):
        table = Table('insert_test', MetaData(bind=engine),
                      Column('a', sqlalchemy.types.Integer),
                      schema='pyhive_test_database')
        table.drop(checkfirst=True)
        table.create()
        connection.execute(table.insert([{'a': 1}, {'a': 2}]))

        result = table.select().execute().fetchall()
        expected = [(1,), (2,)]
        self.assertEqual(result, expected)

    @with_engine_connection
    def test_supports_san_rowcount(self, engine, connection):
        self.assertFalse(engine.dialect.supports_sane_rowcount_returning)
