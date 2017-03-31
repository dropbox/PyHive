"""Integration between SQLAlchemy and Athena.

Some code based on
https://github.com/zzzeek/sqlalchemy/blob/rel_0_5/lib/sqlalchemy/databases/sqlite.py
which is released under the MIT license.
"""

from __future__ import absolute_import
from __future__ import unicode_literals
import re
from distutils.version import StrictVersion
from pyhive import presto
from pyhive.common import UniversalSet
from sqlalchemy import exc
from sqlalchemy import types
#from sqlalchemy import util
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
import sqlalchemy

from pyathenajdbc.converter import JDBCTypeConverter

try:
    from sqlalchemy.sql.compiler import SQLCompiler
except ImportError:
    from sqlalchemy.sql.compiler import DefaultCompiler as SQLCompiler


class AthenaIdentifierPreparer(compiler.IdentifierPreparer):
    # Just quote everything to make things simpler / easier to upgrade
    reserved_words = UniversalSet()

_type_map = {
    'NULL': types.NullType,
    'BOOLEAN': types.Boolean,
    'TINYINT': types.Integer,
    'SMALLINT': types.Integer,
    'BIGINT': types.BigInteger,
    'INTEGER': types.Integer,
    'REAL': types.Float,
    'DOUBLE': types.Float,
    'FLOAT': types.Float,
    'CHAR': types.String,
    'NCHAR': types.String,
    'VARCHAR': types.String,
    'NVARCHAR': types.String,
    'LONGVARCHAR': types.String,
    'LONGNVARCHAR': types.String,
    'DATE': types.DATE,
    'TIMESTAMP': types.TIMESTAMP,
    'TIMESTAMP_WITH_TIMEZONE': types.TIMESTAMP,
    'ARRAY': types.ARRAY,
    'DECIMAL': types.DECIMAL,
    'NUMERIC': types.Numeric,
    'BINARY': types.Binary,
    'VARBINARY': types.Binary,
    'LONGVARBINARY': types.Binary,
    # TODO Converter impl
    # 'TIME': ???,
    # 'BIT': ???,
    # 'CLOB': ???,
    'BLOB': types.BLOB,
    # 'NCLOB': ???,
    # 'STRUCT': ???,
     'JAVA_OBJECT': types.BLOB,
    # 'REF_CURSOR': ???,
    # 'REF': ???,
    # 'DISTINCT': ???,
    # 'DATALINK': ???,
    # 'SQLXML': ???,
    # 'OTHER': ???,
    # 'ROWID': ???,
}


class AthenaCompiler(SQLCompiler):
    def visit_char_length_func(self, fn, **kw):
        return 'length{}'.format(self.function_argspec(fn, **kw))


class AthenaDialect(default.DefaultDialect):
    name = 'athena'
    driver = 'rest'
    preparer = AthenaIdentifierPreparer
    statement_compiler = AthenaCompiler
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True

    jdbctypeconverter = None
    jdbc_type_map = None


    @classmethod
    def dbapi(cls):
        import pyathenajdbc
        import pyathenajdbc.error

        pyathenajdbc.Error = pyathenajdbc.error.Error
        pyathenajdbc.Warning = pyathenajdbc.error.Warning
        pyathenajdbc.InterfaceError = pyathenajdbc.error.InterfaceError
        pyathenajdbc.DatabaseError = pyathenajdbc.error.DatabaseError
        pyathenajdbc.InternalError = pyathenajdbc.error.InternalError
        pyathenajdbc.OperationalError = pyathenajdbc.error.OperationalError
        pyathenajdbc.ProgrammingError = pyathenajdbc.error.ProgrammingError
        pyathenajdbc.IntegrityError = pyathenajdbc.error.IntegrityError
        pyathenajdbc.DataError = pyathenajdbc.error.DataError
        pyathenajdbc.NotSupportedError = pyathenajdbc.error.NotSupportedError

        return pyathenajdbc

    def create_connect_args(self, url):
        db_parts = (url.database or 'hive').split('/')

        # TODO:
        # - schema_name='default'
        # - profile_name=None
        # - credential_file=None
        kwargs = {
            'host': url.host,
            'access_key': url.username,
            'secret_key': url.password,
            'region_name': url.query['region_name'],
            's3_staging_dir': url.query['s3_staging_dir']
        }
        kwargs.update(url.query)
        if len(db_parts) == 1:
            kwargs['catalog'] = db_parts[0]
        elif len(db_parts) == 2:
            kwargs['catalog'] = db_parts[0]
            kwargs['schema'] = db_parts[1]
        else:
            raise ValueError("Unexpected database format {}".format(url.database))
        return ([], kwargs)

    def get_schema_names(self, connection, **kw):
        return [schema for (schema,) in connection.execute('SHOW SCHEMAS')]

    def _get_table_columns(self, connection, table_name, schema):
        name = table_name
        if schema is not None:
            name = '%s.%s' % (schema, name)
        try:
            return connection.execute('SHOW COLUMNS IN {}'.format(name))
        except (presto.DatabaseError, exc.DatabaseError) as e:
            # Normally SQLAlchemy should wrap this exception in sqlalchemy.exc.DatabaseError, which
            # it successfully does in the Hive version. The difference with Athena is that this
            # error is raised when fetching the cursor's description rather than the initial execute
            # call. SQLAlchemy doesn't handle this. Thus, we catch the unwrapped
            # presto.DatabaseError here.
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

    def get_columns(self, connection, table_name, schema=None, **kwargs):

        if self.jdbctypeconverter is None:
            self.jdbctypeconverter = JDBCTypeConverter()
            self.jdbc_type_map = {v: k for (k,v) in self.jdbctypeconverter.jdbc_type_mappings.items()}

        # pylint: disable=unused-argument
        name = table_name
        if schema is not None:
            name = '%s.%s' % (schema, name)
        query = 'SELECT * FROM %s LIMIT 0' % name
        cursor = connection.execute(query)
        schema = cursor.cursor.description
        # We need to fetch the empty results otherwise these queries remain in
        # flight
        cursor.fetchall()
        column_info = []
        for col in schema:
            column_info.append({
                'name': col[0],
                'type': _type_map[self.jdbc_type_map[col[1]]],
                'nullable': True,
                'autoincrement': False})
        return column_info
    
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_table_names(self, connection, schema=None, **kw):
        query = 'SHOW TABLES'
        if schema:
            query += ' IN {}'.format(schema)
        return [tbl for (tbl,) in connection.execute(query).fetchall()]

    def do_rollback(self, dbapi_connection):
        # No transactions for Athena
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # requests gives back Unicode strings
        return True

    def _check_unicode_description(self, connection):
        # requests gives back Unicode strings
        return True

if StrictVersion(sqlalchemy.__version__) < StrictVersion('0.7.0'):
    from pyhive import sqlalchemy_backports

    def reflecttable(self, connection, table, include_columns=None, exclude_columns=None):
        insp = sqlalchemy_backports.Inspector.from_engine(connection)
        return insp.reflecttable(table, include_columns, exclude_columns)
    AthenaDialect.reflecttable = reflecttable
