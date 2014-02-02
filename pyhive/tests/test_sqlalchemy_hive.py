from pyhive.tests.sqlachemy_test_case import SqlAlchemyTestCase
from pyhive.tests.sqlachemy_test_case import with_engine_connection
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Column
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import Table
from sqlalchemy.types import String
import datetime
import decimal


class TestSqlAlchemyHive(SqlAlchemyTestCase):
    __test__ = True

    def create_engine(self):
        return create_engine('hive://hadoop@localhost:10000/')

    @with_engine_connection
    def test_reflect_select(self, engine, connection):
        """reflecttable should be able to fill in a table from the name"""
        one_row_complex = Table('one_row_complex', MetaData(bind=engine), autoload=True)
        self.assertEqual(len(one_row_complex.c), 15)
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
            datetime.datetime(1970, 1, 1, 0, 0),
            '123',
            '[1,2]',
            '{1:2,3:4}',
            '{"a":1,"b":2}',
            '{0:1}',
            decimal.Decimal('0.1'),
        ])

    @with_engine_connection
    def test_reserved_words(self, engine, connection):
        """Hive uses backticks"""
        # Use keywords for the table/column name
        fake_table = Table('select', MetaData(bind=engine), Column('map', String))
        query = str(fake_table.select(fake_table.c.map == 'a'))
        self.assertIn('`select`', query)
        self.assertIn('`map`', query)
        self.assertNotIn('"select"', query)
        self.assertNotIn('"map"', query)
