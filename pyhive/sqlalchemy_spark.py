"""Integration between SQLAlchemy and Hive.

Some code based on
https://github.com/zzzeek/sqlalchemy/blob/rel_0_5/lib/sqlalchemy/databases/sqlite.py
which is released under the MIT license.
"""

from __future__ import absolute_import
from __future__ import unicode_literals
from pyhive.sqlalchemy_hive import HiveDialect, StrictVersion, HiveTypeCompiler
import sqlalchemy


class SparkDialect(HiveDialect):
    name = b'spark'

    def get_schema_names(self, connection, **kw):
        # Equivalent to SHOW DATABASES
        return [row.databaseName for row in connection.execute('SHOW SCHEMAS')]

    def get_columns(self, connection, table_name, schema=None, **kw):
        if self.is_temporary(connection, schema, table_name):
            schema = None
        return super(SparkDialect, self).get_columns(connection, table_name, schema, **kw)

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_table_names(self, connection, schema=None, **kw):
        query_result = self._get_table_metadata(connection, schema)
        # Temporary Views do not really belong to a schema, even though they show up in every schema
        return [row.tableName for row in query_result
                if not (row.tableName is not None and row.isTemporary)]

    def is_temporary(self, connection, schema, table_name):
        query_result = self._get_table_metadata(connection, schema)
        table_temporary_data = [row.isTemporary for row in query_result
                                if row.tableName == table_name]
        return len(table_temporary_data) > 0 and table_temporary_data[0]

    def _get_table_metadata(self, connection, schema):
        query = 'SHOW TABLES'
        if schema:
            query += ' IN ' + self.identifier_preparer.quote_identifier(schema)
        return connection.execute(query)

if StrictVersion(sqlalchemy.__version__) < StrictVersion('0.7.0'):
    from pyhive import sqlalchemy_backports

    def reflecttable(self, connection, table, include_columns=None, exclude_columns=None):
        insp = sqlalchemy_backports.Inspector.from_engine(connection)
        return insp.reflecttable(table, include_columns, exclude_columns)
    HiveDialect.reflecttable = reflecttable
else:
    HiveDialect.type_compiler = HiveTypeCompiler
