"""Integration between SQLAlchemy and Hive.

Some code based on
https://github.com/zzzeek/sqlalchemy/blob/rel_0_5/lib/sqlalchemy/databases/sqlite.py
which is released under the MIT license.
"""

from __future__ import absolute_import
from __future__ import unicode_literals
from pyhive import hive
from sqlalchemy import exc
from sqlalchemy import schema
from sqlalchemy import types
from sqlalchemy import util
from sqlalchemy.databases import mysql
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
import decimal
import re

try:
    from sqlalchemy import processors
except ImportError:
    from pyhive import sqlalchemy_processors as processors


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


class HiveDialect(default.DefaultDialect):
    name = 'hive'
    driver = 'thrift'
    preparer = HiveIdentifierPreparer
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

    def reflecttable(self, connection, table, include_columns=None, exclude_columns=None):
        exclude_columns = exclude_columns or []
        # TODO using TGetColumnsReq hangs after sending TFetchResultsReq.
        # Using DESCRIBE works but is uglier.
        try:
            # This needs the table name to be unescaped (no backticks).
            rows = connection.execute('DESCRIBE {}'.format(table)).fetchall()
        except exc.OperationalError as e:
            # Does the table exist?
            regex_fmt = r'TExecuteStatementResp.*SemanticException.*Table not found {}'
            regex = regex_fmt.format(re.escape(table.name))
            if re.search(regex, e.message):
                raise exc.NoSuchTableError(table.name)
            else:
                raise
        else:
            # Strip whitespace
            rows = [[col.strip() if col else None for col in row] for row in rows]
            # Filter out empty rows and comment
            rows = [row for row in rows if row[0] and row[0] != '# col_name']
            for i, (col_name, col_type, _comment) in enumerate(rows):
                if col_name == '# Partition Information':
                    break
                # Take out the more detailed type information
                # e.g. 'map<int,int>' -> 'map'
                col_type = col_type.partition('<')[0]
                if include_columns is not None and col_name not in include_columns:
                    continue
                if col_name in exclude_columns:
                    continue
                try:
                    coltype = _type_map[col_type]
                except KeyError:
                    util.warn("Did not recognize type '%s' of column '%s'" % (
                        col_type, col_name))
                    coltype = types.NullType
                table.append_column(schema.Column(
                    name=col_name,
                    type_=coltype,
                ))
            # Handle partition columns
            for col_name, col_type, _comment in rows[i + 1:]:
                if include_columns is not None and col_name not in include_columns:
                    continue
                if col_name in exclude_columns:
                    continue
                getattr(table.c, col_name).index = True

    def do_rollback(self, dbapi_connection):
        # No transactions for Hive
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # We decode everything as UTF-8
        return True

    def _check_unicode_description(self, connection):
        # We decode everything as UTF-8
        return True
