from pyhive.tests.sqlachemy_test_case import SqlAlchemyTestCase
from pyhive.tests.sqlachemy_test_case import with_engine_connection
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Column
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import Table
from sqlalchemy.types import String
import contextlib
# This import has the side effect of registering with sqlalchemy
import pyhive.sqlalchemy_presto


class TestSqlAlchemyPresto(SqlAlchemyTestCase):
    __test__ = True

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
    def test_reserved_words(self, engine, connection):
        """Presto uses double quotes, not backticks"""
        # Use keywords for the table/column name
        fake_table = Table('select', MetaData(bind=engine), Column('current_timestamp', String))
        query = str(fake_table.select(fake_table.c.current_timestamp == 'a'))
        self.assertIn('"select"', query)
        self.assertIn('"current_timestamp"', query)
        self.assertNotIn('`select`', query)
        self.assertNotIn('`current_timestamp`', query)
