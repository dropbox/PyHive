from . import sqlalchemy_hive

import re

from sqlalchemy import exc
from sqlalchemy.engine import default


class SparkSqlDialect(sqlalchemy_hive.HiveDialect):
    name = b'sparksql'
    execution_ctx_cls = default.DefaultExecutionContext

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
            regex_fmt = r'TExecuteStatementResp.*NoSuchTableException.*Table or view \'{}\'' \
                        r' not found'
            regex = regex_fmt.format(re.escape(table_name))
            if re.search(regex, e.args[0]):
                raise exc.NoSuchTableError(full_table)
            elif schema:
                schema_regex_fmt = r'TExecuteStatementResp.*NoSuchDatabaseException.*Database ' \
                                   r'\'{}\' not found'
                schema_regex = schema_regex_fmt.format(re.escape(schema))
                if re.search(schema_regex, e.args[0]):
                    raise exc.NoSuchTableError(full_table)
            else:
                # When a hive-only column exists in a table
                hive_regex_fmt = r'org.apache.spark.SparkException: Cannot recognize hive type ' \
                                 r'string'
                if re.search(hive_regex_fmt, e.args[0]):
                    raise exc.UnreflectableTableError
                else:
                    raise
        else:
            return rows

    def get_table_names(self, connection, schema=None, **kw):
        query = 'SHOW TABLES'
        if schema:
            query += ' IN ' + self.identifier_preparer.quote_identifier(schema)
        return list(row[1] for row in filter(
            lambda x: not x[-1],
            [row for row in connection.execute(query)]
        ))

    def get_view_names(self, connection, schema=None, **kw):
        query = 'SHOW TABLES'
        if schema:
            query += ' IN ' + self.identifier_preparer.quote_identifier(schema)
        return list(row[1] for row in filter(
            lambda x: x[-1],
            [row for row in connection.execute(query)]
        ))

    def has_table(self, connection, table_name, schema=None):
        try:
            self._get_table_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError:
            return False
        except exc.UnreflectableTableError:
            return False
