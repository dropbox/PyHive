from __future__ import absolute_import
from __future__ import unicode_literals
from distutils.version import StrictVersion
from sqlalchemy import types as sql_types
import sqlalchemy as sa

if StrictVersion(sa.__version__) >= StrictVersion('0.9.4'):
    from sqlalchemy.testing import suite
    from sqlalchemy.testing.suite import *

    class ComponentReflectionTest(suite.ComponentReflectionTest):
        @classmethod
        def define_reflected_tables(cls, metadata, schema):
            users = Table('users', metadata,
                Column('user_id', sa.INT),
                Column('test1', sa.CHAR(5)),
                Column('test2', sa.Float(5)),
                schema=schema,
            )
            Table("dingalings", metadata,
                  Column('dingaling_id', sa.Integer),
                  Column('address_id', sa.Integer),
                  Column('data', sa.String(30)),
                  schema=schema,
            )
            Table('email_addresses', metadata,
                Column('address_id', sa.Integer),
                Column('remote_user_id', sa.Integer),
                Column('email_address', sa.String(20)),
                schema=schema,
            )

            if testing.requires.index_reflection.enabled:
                cls.define_index(metadata, users)
            if testing.requires.view_reflection.enabled:
                cls.define_views(metadata, schema)

        def test_nullable_reflection(self):
            # TODO figure out why pytest treats unittest.skip as a failure
            pass

        def test_numeric_reflection(self):
            # TODO figure out why pytest treats unittest.skip as a failure
            pass

        def test_varchar_reflection(self):
            typ = self._type_round_trip(sql_types.String(52))[0]
            assert isinstance(typ, sql_types.String)

    class HasTableTest(suite.HasTableTest):
        @classmethod
        def define_tables(cls, metadata):
            Table('test_table', metadata,
                Column('id', Integer),
                Column('data', String(50))
            )

    class OrderByLabelTest(suite.OrderByLabelTest):
        _ran_insert_data = False

        @classmethod
        def define_tables(cls, metadata):
            Table("some_table", metadata,
                Column('id', Integer),
                Column('x', Integer),
                Column('y', Integer),
                Column('q', String(50)),
                Column('p', String(50))
            )

        @classmethod
        def insert_data(cls):
            if not cls._ran_insert_data:  # MapReduce is slow
                cls._ran_insert_data = True
                config.db.execute('''
                INSERT OVERWRITE TABLE some_table SELECT stack(3,
                    1, 1, 2, 'q1', 'p3',
                    2, 2, 3, 'q2', 'p2',
                    3, 3, 4, 'q3', 'p1'
                ) AS (id, x, y, q, p) FROM default.one_row
                ''')

    class TableDDLTest(fixtures.TestBase):
        def _simple_fixture(self):
            return Table('test_table', self.metadata,
                Column('id', Integer),
                Column('data', String(50))
            )

        def _simple_roundtrip(self, table):
            # Inserting data into Hive is hard.
            pass

    # These test rely on inserting data, which is hard in Hive.
    # TODO could in theory compile insert statements using insert select from a known one row table.
    BooleanTest = None
    DateTimeMicrosecondsTest = None
    DateTimeTest = None
    InsertBehaviorTest = None
    IntegerTest = None
    NumericTest = None
    RowFetchTest = None
    SimpleUpdateDeleteTest = None
    StringTest = None
    TextTest = None
    TimeMicrosecondsTest = None
    TimeTest = None
    UnicodeTextTest = None
    UnicodeVarcharTest = None
