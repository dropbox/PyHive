"""Integration between SQLAlchemy and Hive.

Some code based on
https://github.com/zzzeek/sqlalchemy/blob/rel_0_5/lib/sqlalchemy/databases/sqlite.py
which is released under the MIT license.
"""

from __future__ import absolute_import
from __future__ import unicode_literals
from distutils.version import StrictVersion
from pyhive import hive
from sqlalchemy.sql import compiler
from sqlalchemy import exc
from sqlalchemy import types
from sqlalchemy import util
from sqlalchemy.databases import mysql
from sqlalchemy.engine import default
import decimal
import re
import sqlalchemy

try:
    from sqlalchemy import processors
except ImportError:
    from pyhive import sqlalchemy_backports as processors
try:
    from sqlalchemy.sql.compiler import SQLCompiler
except ImportError:
    from sqlalchemy.sql.compiler import DefaultCompiler as SQLCompiler


class HiveStringTypeBase(types.TypeDecorator):
    """Translates strings returned by Thrift into something else"""
    impl = types.String

    def process_bind_param(self, value, dialect):
        raise NotImplementedError("Writing to Hive not supported")


class HiveTimestamp(HiveStringTypeBase):
    """Translates timestamp strings to datetime objects"""

    def process_result_value(self, value, dialect):
        return processors.str_to_datetime(value)


class HiveDecimal(HiveStringTypeBase):
    """Translates strings to decimals"""

    def process_result_value(self, value, dialect):
        return decimal.Decimal(value)


def _get_illegal_initial_characters():
    if isinstance(compiler.IdentifierPreparer.illegal_initial_characters, set):
        # Newer sqlalchemy
        return set([str(x) for x in range(0, 10)]).union(['$', '_'])
    else:
        # For backwards compatibility with 0.5 (and maybe others)
        return re.compile(r'[0-9$_]')


class HiveIdentifierPreparer(compiler.IdentifierPreparer):
    # https://github.com/apache/hive/blob/trunk/ql/src/java/org/apache/hadoop/hive/ql/parse/HiveLexer.g
    reserved_words = frozenset([
        '$elem$',
        '$key$',
        '$value$',
        'add',
        'admin'
        'after',
        'all',
        'alter',
        'analyze',
        'and',
        'archive',
        'array',
        'as',
        'asc',
        'before',
        'between',
        'bigint',
        'binary',
        'boolean',
        'both',
        'bucket',
        'buckets',
        'by',
        'cascade',
        'case',
        'cast',
        'change',
        'char',
        'cluster',
        'clustered',
        'clusterstatus',
        'collection',
        'column',
        'columns',
        'comment',
        'compute',
        'concatenate',
        'continue',
        'create',
        'cross',
        'cube',
        'current',
        'cursor',
        'data',
        'database',
        'databases',
        'date',
        'datetime',
        'dbproperties',
        'decimal',
        'deferred',
        'defined',
        'delete',
        'delimited',
        'dependency',
        'desc',
        'describe',
        'directories',
        'directory',
        'disable',
        'distinct',
        'distribute',
        'double',
        'drop',
        'else',
        'enable',
        'end',
        'escaped',
        'exchange',
        'exclusive',
        'exists',
        'explain',
        'export',
        'extended',
        'external',
        'false',
        'fetch',
        'fields',
        'fileformat',
        'first',
        'float',
        'following',
        'for',
        'format',
        'formatted',
        'from',
        'full',
        'function',
        'functions',
        'grant',
        'group',
        'grouping',
        'having',
        'hold_ddltime',
        'idxproperties',
        'if',
        'ignore',
        'import',
        'in',
        'index',
        'indexes',
        'inner',
        'inpath',
        'inputdriver',
        'inputformat',
        'insert',
        'int',
        'intersect',
        'into',
        'is',
        'items',
        'join',
        'keys',
        'lateral',
        'left',
        'less',
        'like',
        'limit',
        'lines',
        'load',
        'local',
        'location',
        'lock',
        'locks',
        'logical',
        'long',
        'macro',
        'map',
        'mapjoin',
        'materialized',
        'minus',
        'more',
        'msck',
        'no_drop',
        'noscan',
        'not',
        'null',
        'of',
        'offline',
        'on',
        'option',
        'or',
        'orc',
        'order',
        'out',
        'outer',
        'outputdriver',
        'outputformat',
        'over',
        'overwrite',
        'partialscan',
        'partition',
        'partitioned',
        'partitions',
        'percent',
        'plus',
        'preceding',
        'preserve',
        'pretty',
        'procedure',
        'protection',
        'purge',
        'range',
        'rcfile',
        'read',
        'readonly',
        'reads',
        'rebuild',
        'recordreader',
        'recordwriter',
        'reduce',
        'regexp',
        'rename',
        'repair',
        'replace',
        'restrict',
        'revoke',
        'right',
        'rlike',
        'role',
        'roles',
        'rollup',
        'row',
        'rows',
        'schema',
        'schemas',
        'select',
        'semi',
        'sequencefile',
        'serde',
        'serdeproperties',
        'set',
        'sets',
        'shared',
        'show',
        'show_database',
        'skewed',
        'smallint',
        'sort',
        'sorted',
        'ssl',
        'statistics',
        'stored',
        'streamtable',
        'string',
        'struct',
        'table',
        'tables',
        'tablesample',
        'tblproperties',
        'temporary',
        'terminated',
        'textfile',
        'then',
        'timestamp',
        'tinyint',
        'to',
        'touch',
        'transform',
        'trigger',
        'true',
        'truncate',
        'unarchive',
        'unbounded',
        'undo',
        'union',
        'uniontype',
        'uniquejoin',
        'unlock',
        'unset',
        'unsigned',
        'update',
        'use',
        'user',
        'using',
        'utc',
        'utc_tmestamp',
        'varchar',
        'view',
        'when',
        'where',
        'while',
        'window',
        'with',
    ])

    legal_characters = re.compile(r'^[A-Z0-9_]+$', re.I)

    illegal_initial_characters = _get_illegal_initial_characters()

    def __init__(self, dialect):
        super(HiveIdentifierPreparer, self).__init__(
            dialect,
            initial_quote='`',
        )


try:
    from sqlalchemy.types import BigInteger
except ImportError:
    from sqlalchemy.databases.mysql import MSBigInteger as BigInteger
_type_map = {
    'boolean': types.Boolean,
    'tinyint': mysql.MSTinyInteger,
    'smallint': types.SmallInteger,
    'int': types.Integer,
    'bigint': BigInteger,
    'float': types.Float,
    'double': types.Float,
    'string': types.String,
    'timestamp': HiveTimestamp,
    'binary': types.String,
    'array': types.String,
    'map': types.String,
    'struct': types.String,
    'uniontype': types.String,
    'decimal': HiveDecimal,
}


class HiveCompiler(SQLCompiler):
    def visit_concat_op_binary(self, binary, operator, **kw):
        return "concat(%s, %s)" % (self.process(binary.left), self.process(binary.right))


if StrictVersion(sqlalchemy.__version__) >= StrictVersion('0.6.0'):
    class HiveTypeCompiler(compiler.GenericTypeCompiler):
        def visit_INTEGER(self, type_):
            return 'INT'

        def visit_CHAR(self, type_):
            return 'STRING'

        def visit_VARCHAR(self, type_):
            return 'STRING'


class HiveDialect(default.DefaultDialect):
    name = b'hive'
    driver = b'thrift'
    preparer = HiveIdentifierPreparer
    statement_compiler = HiveCompiler
    supports_alter = True
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_native_decimal = True
    supports_native_boolean = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    dbapi_type_map = {
        'TIMESTAMP_TYPE': HiveTimestamp(),
        'DECIMAL_TYPE': HiveDecimal(),
    }

    @classmethod
    def dbapi(cls):
        return hive

    def create_connect_args(self, url):
        kwargs = {
            'host': url.host,
            'port': url.port,
            'username': url.username,
            'database': url.database,
        }
        kwargs.update(url.query)
        return ([], kwargs)

    def get_schema_names(self, connection, **kw):
        # Equivalent to SHOW DATABASES
        return [row.database_name for row in connection.execute('SHOW SCHEMAS')]

    def _get_table_columns(self, connection, table_name, schema):
        full_table = table_name
        if schema:
            full_table = schema + '.' + table_name
        # TODO using TGetColumnsReq hangs after sending TFetchResultsReq.
        # Using DESCRIBE works but is uglier.
        try:
            # This needs the table name to be unescaped (no backticks).
            rows = connection.execute('DESCRIBE {}'.format(full_table)).fetchall()
        except exc.OperationalError as e:
            # Does the table exist?
            regex_fmt = r'TExecuteStatementResp.*SemanticException.*Table not found {}'
            regex = regex_fmt.format(re.escape(full_table))
            if re.search(regex, e.message):
                raise exc.NoSuchTableError(full_table)
            else:
                raise
        else:
            # Hive is stupid: this is what I get from DESCRIBE some_schema.does_not_exist
            regex = r'Table .* does not exist'
            if len(rows) == 1 and re.match(regex, rows[0].col_name):
                raise exc.NoSuchTableError(full_table)
            return rows

    def has_table(self, connection, table_name, schema=None):
        try:
            self._get_table_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError:
            return False

    def get_columns(self, connection, table_name, schema=None, **kw):
        rows = self._get_table_columns(connection, table_name, schema)
        # Strip whitespace
        rows = [[col.strip() if col else None for col in row] for row in rows]
        # Filter out empty rows and comment
        rows = [row for row in rows if row[0] and row[0] != '# col_name']
        result = []
        for (col_name, col_type, _comment) in rows:
            if col_name == '# Partition Information':
                break
            # Take out the more detailed type information
            # e.g. 'map<int,int>' -> 'map'
            col_type = col_type.partition('<')[0]
            try:
                coltype = _type_map[col_type]
            except KeyError:
                util.warn("Did not recognize type '%s' of column '%s'" % (
                    col_type, col_name))
                coltype = types.NullType
            result.append({
                'name': col_name,
                'type': coltype,
                'nullable': True,
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
        # Strip whitespace
        rows = [[col.strip() if col else None for col in row] for row in rows]
        # Filter out empty rows and comment
        rows = [row for row in rows if row[0] and row[0] != '# col_name']
        for i, (col_name, _col_type, _comment) in enumerate(rows):
            if col_name == '# Partition Information':
                break
        # Handle partition columns
        col_names = []
        for col_name, _col_type, _comment in rows[i + 1:]:
            col_names.append(col_name)
        if col_names:
            return [{'name': 'partition', 'column_names': col_names, 'unique': False}]
        else:
            return []

    def get_table_names(self, connection, schema=None, **kw):
        query = 'SHOW TABLES'
        if schema:
            query += ' IN ' + self.identifier_preparer.quote_identifier(schema)
        return [row.tab_name for row in connection.execute(query)]

    def do_rollback(self, dbapi_connection):
        # No transactions for Hive
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # We decode everything as UTF-8
        return True

    def _check_unicode_description(self, connection):
        # We decode everything as UTF-8
        return True

if StrictVersion(sqlalchemy.__version__) < StrictVersion('0.6.0'):
    from pyhive import sqlalchemy_backports

    def reflecttable(self, connection, table, include_columns=None, exclude_columns=None):
        insp = sqlalchemy_backports.Inspector.from_engine(connection)
        return insp.reflecttable(table, include_columns, exclude_columns)
    HiveDialect.reflecttable = reflecttable
else:
    HiveDialect.type_compiler = HiveTypeCompiler
