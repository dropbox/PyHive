"""Integration between SQLAlchemy and Presto.

Some code based on
https://github.com/zzzeek/sqlalchemy/blob/rel_0_5/lib/sqlalchemy/databases/sqlite.py
which is released under the MIT license.
"""

from __future__ import absolute_import
from __future__ import unicode_literals
from pyhive import presto
from sqlalchemy import exc
from sqlalchemy import schema
from sqlalchemy import types
from sqlalchemy import util
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
import re


class PrestoIdentifierPreparer(compiler.IdentifierPreparer):
    # https://github.com/facebook/presto/blob/master/presto-parser/src/main/antlr3/com/facebook/presto/sql/parser/Statement.g
    reserved_words = frozenset([
        'all',
        'and',
        'as',
        'asc',
        'between',
        'bigint',
        'boolean',
        'by',
        'case',
        'cast',
        'char',
        'character',
        'coalesce',
        'constraint',
        'create',
        'cross',
        'current_date',
        'current_time',
        'current_timestamp',
        'dec',
        'decimal',
        'desc',
        'describe',
        'distinct',
        'double',
        'drop',
        'else',
        'end',
        'escape',
        'except',
        'exists',
        'extract',
        'false',
        'first',
        'for',
        'from',
        'full',
        'group',
        'having',
        'if',
        'in',
        'inner',
        'int',
        'integer',
        'intersect',
        'is',
        'join',
        'last',
        'left',
        'like',
        'limit',
        'natural',
        'not',
        'null',
        'nullif',
        'nulls',
        'number',
        'numeric',
        'on',
        'or',
        'order',
        'outer',
        'recursive',
        'right',
        'select',
        'stratify',
        'substring',
        'table',
        'then',
        'true',
        'unbounded',
        'union',
        'using',
        'varchar',
        'varying',
        'when',
        'where',
        'with',
    ])


try:
    from sqlalchemy.types import BigInteger
except ImportError:
    from sqlalchemy.databases.mysql import MSBigInteger as BigInteger
_type_map = {
    'bigint': BigInteger,
    'boolean': types.Boolean,
    'double': types.Float,
    'varchar': types.String,
}


class PrestoDialect(default.DefaultDialect):
    name = 'presto'
    driver = 'rest'
    preparer = PrestoIdentifierPreparer
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None

    @classmethod
    def dbapi(cls):
        return presto

    def create_connect_args(self, url):
        db_parts = url.database.split('/')
        kwargs = {
            'host': url.host,
            'port': url.port,
            'username': url.username,
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

    def reflecttable(self, connection, table, include_columns=None, exclude_columns=None):
        exclude_columns = exclude_columns or []
        try:
            rows = connection.execute('SHOW COLUMNS FROM "{}"'.format(table))
        except presto.DatabaseError as e:
            # Normally SQLAlchemy should wrap this exception in sqlalchemy.exc.DatabaseError, which
            # it successfully does in the Hive version. The difference with Presto is that this
            # error is raised when fetching the cursor's description rather than the initial execute
            # call. SQLAlchemy doesn't handle this. Thus, we catch the unwrapped
            # presto.DatabaseError here.
            # Does the table exist?
            msg = e.message.get('message') if isinstance(e.message, dict) else None
            regex = r"^Table\ \'.*{}\'\ does\ not\ exist$".format(re.escape(table.name))
            if msg and re.match(regex, msg):
                raise exc.NoSuchTableError(table.name)
            else:
                raise
        else:
            for row in rows:
                name, coltype, nullable, is_partition_key = row
                if include_columns is not None and name not in include_columns:
                    continue
                if name in exclude_columns:
                    continue
                try:
                    coltype = _type_map[coltype]
                except KeyError:
                    util.warn("Did not recognize type '%s' of column '%s'" % (coltype, name))
                    coltype = types.NullType
                table.append_column(schema.Column(
                    name=name,
                    type_=coltype,
                    nullable=nullable,
                    index=is_partition_key,  # Translate Hive partitions to indexes
                ))

    def do_rollback(self, dbapi_connection):
        # No transactions for Presto
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # requests gives back Unicode strings
        return True

    def _check_unicode_description(self, connection):
        # requests gives back Unicode strings
        return True
