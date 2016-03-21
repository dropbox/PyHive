"""Presto integration tests.

These rely on having a Presto+Hadoop cluster set up.
They also require a tables created by make_test_tables.sh.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib

from pyhive import exc
from pyhive import presto
from pyhive.tests.dbapi_test_case import DBAPITestCase
from pyhive.tests.dbapi_test_case import with_cursor
import mock
import unittest

_HOST = 'localhost'


class TestPresto(unittest.TestCase, DBAPITestCase):
    __test__ = True

    def connect(self):
        return presto.connect(host=_HOST, source=self.id())

    @with_cursor
    def test_description(self, cursor):
        cursor.execute('SELECT 1 AS foobar FROM one_row')
        self.assertEqual(cursor.description, [('foobar', 'bigint', None, None, None, None, True)])

    @with_cursor
    def test_complex(self, cursor):
        cursor.execute('SELECT * FROM one_row_complex')
        # TODO delete this code after dropping test support for older presto
        # old presto uses <>, while new presto uses ()
        description = []
        for row in cursor.description:
            description.append((row[0], row[1].replace('<', '(').replace('>', ')')) + row[2:])
        # TODO Presto drops the union and decimal fields
        self.assertEqual(description, [
            ('boolean', 'boolean', None, None, None, None, True),
            ('tinyint', 'bigint', None, None, None, None, True),
            ('smallint', 'bigint', None, None, None, None, True),
            ('int', 'bigint', None, None, None, None, True),
            ('bigint', 'bigint', None, None, None, None, True),
            ('float', 'double', None, None, None, None, True),
            ('double', 'double', None, None, None, None, True),
            ('string', 'varchar', None, None, None, None, True),
            ('timestamp', 'timestamp', None, None, None, None, True),
            ('binary', 'varbinary', None, None, None, None, True),
            ('array', 'array(bigint)', None, None, None, None, True),
            ('map', 'map(bigint,bigint)', None, None, None, None, True),
            ('struct', "row(bigint,bigint)('a','b')", None, None, None, None, True),
            #('union', 'varchar', None, None, None, None, True),
            #('decimal', 'double', None, None, None, None, True),
        ])
        self.assertEqual(cursor.fetchall(), [[
            True,
            127,
            32767,
            2147483647,
            9223372036854775807,
            0.5,
            0.25,
            'a string',
            '1970-01-01 00:00:00.000',
            '123',
            [1, 2],
            {"1": 2, "3": 4},  # Presto converts all keys to strings so that they're valid JSON
            [1, 2],  # struct is returned as a list of elements
            #'{0:1}',
            #0.1,
        ]])

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

    @with_cursor
    def test_poll(self, cursor):
        self.assertRaises(presto.ProgrammingError, cursor.poll)

        cursor.execute('SELECT * FROM one_row')
        while True:
            status = cursor.poll()
            if status is None:
                break
            self.assertIn('stats', status)

        def fail(*args, **kwargs):
            self.fail("Should not need requests.get after done polling")  # pragma: no cover
        with mock.patch('requests.get', fail):
            self.assertEqual(cursor.fetchall(), [[1]])

    @with_cursor
    def test_set_session(self, cursor):
        cursor.execute("SET SESSION query_max_run_time = '1234m'")
        cursor.fetchall()

        cursor.execute('SHOW SESSION')
        rows = [r for r in cursor.fetchall() if r[0] == 'query_max_run_time']
        assert len(rows) == 1
        session_prop = rows[0]
        assert session_prop[1] == '1234m'

        cursor.execute('RESET SESSION query_max_run_time')
        cursor.fetchall()

        cursor.execute('SHOW SESSION')
        rows = [r for r in cursor.fetchall() if r[0] == 'query_max_run_time']
        assert len(rows) == 1
        session_prop = rows[0]
        assert session_prop[1] != '1234m'

    def test_set_session_in_consructor(self):
        conn = presto.connect(
            host=_HOST, source=self.id(), session_props={'query_max_run_time': '1234m'}
        )
        with contextlib.closing(conn):
            with contextlib.closing(conn.cursor()) as cursor:
                cursor.execute('SHOW SESSION')
                rows = [r for r in cursor.fetchall() if r[0] == 'query_max_run_time']
                assert len(rows) == 1
                session_prop = rows[0]
                assert session_prop[1] == '1234m'

                cursor.execute('RESET SESSION query_max_run_time')
                cursor.fetchall()

                cursor.execute('SHOW SESSION')
                rows = [r for r in cursor.fetchall() if r[0] == 'query_max_run_time']
                assert len(rows) == 1
                session_prop = rows[0]
                assert session_prop[1] != '1234m'
