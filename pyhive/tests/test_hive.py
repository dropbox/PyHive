"""Hive integration tests.

These rely on having a Hive+Hadoop cluster set up with HiveServer2 running.
They also require a tables created by make_test_tables.sh.
"""
from TCLIService import ttypes
from pyhive import exc
from pyhive import hive
from pyhive.tests.dbapi_test_case import DBAPITestCase
import mock
import contextlib
from pyhive.tests.dbapi_test_case import with_cursor

_HOST = 'localhost'


class TestHive(DBAPITestCase):
    __test__ = True

    def connect(self):
        return hive.connect(host=_HOST, username='hadoop')

    @with_cursor
    def test_description(self, cursor):
        cursor.execute('SELECT * FROM one_row')
        desc = [('number_of_rows', 'INT_TYPE', None, None, None, None, True)]
        self.assertEqual(cursor.description, desc)
        self.assertEqual(cursor.description, desc)

    @with_cursor
    def test_complex(self, cursor):
        cursor.execute('SELECT * FROM one_row_complex')
        self.assertEqual(cursor.description, [
            ('a', 'STRING_TYPE', None, None, None, None, True),
            ('b', 'STRING_TYPE', None, None, None, None, True),
        ])
        self.assertEqual(cursor.fetchall(), [['{1:"a",2:"b"}', '[1,2,3]']])

    def test_noops(self):
        """The DB-API specification requires that certain actions exist, even though they might not
        be applicable."""
        # Wohoo inflating coverage stats!
        with contextlib.closing(self.connect()) as connection:
            with contextlib.closing(connection.cursor()) as cursor:
                self.assertEqual(cursor.rowcount, -1)
                cursor.setinputsizes([])
                cursor.setoutputsize(1, 'blah')
                connection.commit()

    @mock.patch('TCLIService.TCLIService.Client.OpenSession')
    def test_open_failed(self, open_session):
        open_session.return_value.serverProtocolVersion = \
            ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1
        self.assertRaises(exc.OperationalError, self.connect)

    def test_escape(self):
        # Hive thrift translates newlines into multiple rows. WTF.
        bad_str = '''`~!@#$%^&*()_+-={}[]|\\;:'",./<>?\t '''
        self.run_escape_case(bad_str)

    def test_newlines(self):
        """Verify that newlines are passed through in a way that doesn't fail parsing"""
        # Hive thrift translates newlines into multiple rows. WTF.
        cursor = self.connect().cursor()
        cursor.execute(
            'SELECT %s FROM one_row',
            (' \r\n \r \n ',)
        )
        self.assertEqual(cursor.fetchall(), [[' '], [' '], [' '], [' ']])
