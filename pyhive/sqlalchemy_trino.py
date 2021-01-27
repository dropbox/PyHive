"""Integration between SQLAlchemy and Trino.

Some code based on
https://github.com/zzzeek/sqlalchemy/blob/rel_0_5/lib/sqlalchemy/databases/sqlite.py
which is released under the MIT license.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import re
from sqlalchemy import exc
from sqlalchemy import types
from sqlalchemy import util
# TODO shouldn't use mysql type
from sqlalchemy.databases import mysql
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy.sql.compiler import SQLCompiler

from pyhive import trino
from pyhive.common import UniversalSet


class TrinoIdentifierPreparer(compiler.IdentifierPreparer):
    # Just quote everything to make things simpler / easier to upgrade
    reserved_words = UniversalSet()


_type_map = {
    'boolean': types.Boolean,
    'tinyint': mysql.MSTinyInteger,
    'smallint': types.SmallInteger,
    'integer': types.Integer,
    'bigint': types.BigInteger,
    'real': types.Float,
    'double': types.Float,
    'varchar': types.String,
    'timestamp': types.TIMESTAMP,
    'date': types.DATE,
    'varbinary': types.VARBINARY,
}


class TrinoCompiler(SQLCompiler):
    def visit_char_length_func(self, fn, **kw):
        return 'length{}'.format(self.function_argspec(fn, **kw))


class TrinoTypeCompiler(compiler.GenericTypeCompiler):
    def visit_CLOB(self, type_, **kw):
        raise ValueError("Trino does not support the CLOB column type.")

    def visit_NCLOB(self, type_, **kw):
        raise ValueError("Trino does not support the NCLOB column type.")

    def visit_DATETIME(self, type_, **kw):
        raise ValueError("Trino does not support the DATETIME column type.")

    def visit_FLOAT(self, type_, **kw):
        return 'DOUBLE'

    def visit_TEXT(self, type_, **kw):
        if type_.length:
            return 'VARCHAR({:d})'.format(type_.length)
        else:
            return 'VARCHAR'


class TrinoDialect(default.DefaultDialect):
    name = 'trino'
    driver = 'rest'
    paramstyle = 'pyformat'
    preparer = TrinoIdentifierPreparer
    statement_compiler = TrinoCompiler
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_multivalues_insert = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True
    type_compiler = TrinoTypeCompiler

    @classmethod
    def dbapi(cls):
        return trino

    def create_connect_args(self, url):
        db_parts = (url.database or 'hive').split('/')
        kwargs = {
            'host': url.host,
            'port': url.port or 8080,
            'username': url.username,
            'password': url.password
        }
        kwargs.update(url.query)
        if len(db_parts) == 1:
            kwargs['catalog'] = db_parts[0]
        elif len(db_parts) == 2:
            kwargs['catalog'] = db_parts[0]
            kwargs['schema'] = db_parts[1]
        else:
            raise ValueError("Unexpected database format {}".format(url.database))
        return [], kwargs

    def get_schema_names(self, connection, **kw):
        return [row.Schema for row in connection.execute('SHOW SCHEMAS')]

    def _get_table_columns(self, connection, table_name, schema):
        full_table = self.identifier_preparer.quote_identifier(table_name)
        if schema:
            full_table = self.identifier_preparer.quote_identifier(schema) + '.' + full_table
        try:
            return connection.execute('SHOW COLUMNS FROM {}'.format(full_table))
        except (trino.DatabaseError, exc.DatabaseError) as e:
            # Normally SQLAlchemy should wrap this exception in sqlalchemy.exc.DatabaseError, which
            # it successfully does in the Hive version. The difference with Trino is that this
            # error is raised when fetching the cursor's description rather than the initial execute
            # call. SQLAlchemy doesn't handle this. Thus, we catch the unwrapped
            # trino.DatabaseError here.
            # Does the table exist?
            msg = (
                e.args[0].get('message') if e.args and isinstance(e.args[0], dict)
                else e.args[0] if e.args and isinstance(e.args[0], str)
                else None
            )
            regex = r"Table\ \'.*{}\'\ does\ not\ exist".format(re.escape(table_name))
            if msg and re.search(regex, msg):
                raise exc.NoSuchTableError(table_name)
            else:
                raise

    def has_table(self, connection, table_name, schema=None):
        try:
            self._get_table_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError:
            return False

    def get_columns(self, connection, table_name, schema=None, **kw):
        rows = self._get_table_columns(connection, table_name, schema)
        result = []
        for row in rows:
            try:
                coltype = _type_map[row.Type]
            except KeyError:
                util.warn("Did not recognize type '%s' of column '%s'" % (row.Type, row.Column))
                coltype = types.NullType
            result.append({
                'name': row.Column,
                'type': coltype,
                # newer Trino no longer includes this column
                'nullable': getattr(row, 'Null', True),
                'default': None,
            })
        return result

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # Hive has no support for foreign keys.
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # Hive has no support for primary keys.
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        rows = self._get_table_columns(connection, table_name, schema)
        col_names = []
        for row in rows:
            part_key = 'Partition Key'
            # Trino puts this information in one of 3 places depending on version
            # - a boolean column named "Partition Key"
            # - a string in the "Comment" column
            # - a string in the "Extra" column
            is_partition_key = (
                (part_key in row and row[part_key])
                or row['Comment'].startswith(part_key)
                or ('Extra' in row and 'partition key' in row['Extra'])
            )
            if is_partition_key:
                col_names.append(row['Column'])
        if col_names:
            return [{'name': 'partition', 'column_names': col_names, 'unique': False}]
        else:
            return []

    def get_table_names(self, connection, schema=None, **kw):
        query = 'SHOW TABLES'
        if schema:
            query += ' FROM ' + self.identifier_preparer.quote_identifier(schema)
        return [row.Table for row in connection.execute(query)]

    def do_rollback(self, dbapi_connection):
        # No transactions for Trino
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # requests gives back Unicode strings
        return True

    def _check_unicode_description(self, connection):
        # requests gives back Unicode strings
        return True
