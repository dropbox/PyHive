"""Presto integration tests.

These rely on having a Presto+Hadoop cluster set up.
They also require a tables created by make_test_tables.sh.
"""
from pyhive import exc
from pyhive import presto
from pyhive.tests.dbapi_test_case import DBAPITestCase
from pyhive.tests.dbapi_test_case import with_cursor
import mock

_HOST = 'localhost'


class TestPresto(DBAPITestCase):
    __test__ = True

    def connect(self):
        return presto.connect(host=_HOST)

    def test_description(self):
        cursor = self.connect().cursor()
        cursor.execute('SELECT 1 AS foobar FROM one_row')
        self.assertEqual(cursor.description, [('foobar', 'bigint', None, None, None, None, True)])

    @with_cursor
    def test_complex(self, cursor):
        cursor.execute('SELECT * FROM one_row_complex')
        self.assertEqual(cursor.description, [
            ('a', 'varchar', None, None, None, None, True),
            ('b', 'varchar', None, None, None, None, True),
        ])
        self.assertEqual(cursor.fetchall(), [['{1:"a",2:"b"}', '[1,2,3]']])

    def test_noops(self):
        """The DB-API specification requires that certain actions exist, even though they might not
        be applicable."""
        # Wohoo inflating coverage stats!
        connection = self.connect()
        cursor = connection.cursor()
        self.assertEqual(cursor.rowcount, -1)
        cursor.setinputsizes([])
        cursor.setoutputsize(1, 'blah')
        connection.commit()

    @mock.patch('requests.post')
    def test_non_200(self, post):
        cursor = self.connect().cursor()
        post.return_value.status_code = 404
        self.assertRaises(exc.OperationalError, lambda: cursor.execute('show tables'))
