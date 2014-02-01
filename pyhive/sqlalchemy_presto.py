"""Integration between SQLAlchemy 0.5.8 and Presto.

On import, this module creates a new module sqlalchemy.databases.presto

Some code based on
https://github.com/zzzeek/sqlalchemy/blob/rel_0_5/lib/sqlalchemy/databases/sqlite.py
which is released under the MIT license.
"""

from pyhive import presto
from sqlalchemy import exc
from sqlalchemy import schema
from sqlalchemy import types
from sqlalchemy import util
from sqlalchemy.databases import mysql
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
import re
import sqlalchemy
import sys


def register_in_sqlalchemy():
    # Register us under SQLAlchemy
    sys.modules['sqlalchemy.databases.presto'] = sys.modules[__name__]
    sqlalchemy.databases.presto = sys.modules[__name__]
register_in_sqlalchemy()


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


_type_map = {
    'bigint': mysql.MSBigInteger,  # In newer SQLAlchemy, this is types.BigInteger
    'boolean': types.Boolean,
    'double': types.Float,
    'varchar': types.String,
}


class PrestoDialect(default.DefaultDialect):
    name = 'presto'
    preparer = PrestoIdentifierPreparer
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False

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
        if len(db_parts) == 1:
            kwargs['catalog'] = db_parts[0]
        elif len(db_parts) == 2:
            kwargs['catalog'] = db_parts[0]
            kwargs['schema'] = db_parts[1]
        else:
            raise ValueError("Unexpected database format {}".format(url.database))
        return ([], kwargs)

    def reflecttable(self, connection, table, include_columns=None):
        try:
            rows = connection.execute('SHOW COLUMNS FROM "{}"'.format(table))
        except presto.DatabaseError as e:
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

dialect = PrestoDialect
