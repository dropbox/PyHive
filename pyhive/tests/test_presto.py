"""Presto integration tests.

These rely on having a Presto+Hadoop cluster set up. They also require a table called one_row.
"""
from pyhive import presto
import mock
import unittest

_HOST = 'localhost'
_ONE_ROW_TABLE_NAME = 'one_row'
_BIG_TABLE_NAME = 'user'


class Testpresto(unittest.TestCase):
    def test_fetchone(self):
        cursor = presto.connect(host=_HOST).cursor()
        cursor.execute('select 1 from {}'.format(_ONE_ROW_TABLE_NAME))
        self.assertEqual(cursor.rownumber, 0)
        self.assertEqual(cursor.fetchone(), [1])
        self.assertEqual(cursor.rownumber, 1)
        self.assertIsNone(cursor.fetchone())

    def test_fetchall(self):
        cursor = presto.connect(host=_HOST).cursor()
        cursor.execute('select count(*) from {}'.format(_ONE_ROW_TABLE_NAME))
        self.assertEqual(cursor.fetchall(), [[1]])

    def test_iterator(self):
        cursor = presto.connect(host=_HOST).cursor()
        cursor.execute('select count(*) from {}'.format(_ONE_ROW_TABLE_NAME))
        self.assertEqual(list(cursor), [[1]])
        self.assertRaises(StopIteration, lambda: cursor.next())

    def test_description(self):
        cursor = presto.connect(host=_HOST).cursor()
        cursor.execute('select 1 as foobar from {}'.format(_ONE_ROW_TABLE_NAME))
        self.assertEqual(cursor.description, [('foobar', 'bigint', None, None, None, None, True)])

    def test_description_initial(self):
        cursor = presto.connect(host=_HOST).cursor()
        self.assertIsNone(cursor.description)

    def test_description_failed(self):
        cursor = presto.connect(host=_HOST).cursor()
        try:
            cursor.execute('blah_blah')
        except presto.DatabaseError:
            pass
        self.assertIsNone(cursor.description)

    def test_bad_query(self):
        cursor = presto.connect(host=_HOST).cursor()

        def run():
            cursor.execute('select does_not_exist from {}'.format(_ONE_ROW_TABLE_NAME))
            cursor.fetchone()
        self.assertRaises(presto.DatabaseError, run)

    def test_concurrent_execution(self):
        cursor = presto.connect(host=_HOST).cursor()
        cursor.execute('select count(*) from {}'.format(_ONE_ROW_TABLE_NAME))
        self.assertRaises(presto.ProgrammingError,
            lambda: cursor.execute('select count(*) from {}'.format(_ONE_ROW_TABLE_NAME)))

    def test_executemany(self):
        cursor = presto.connect(host=_HOST).cursor()
        cursor.executemany(
            'select %(x)d from {}'.format(_ONE_ROW_TABLE_NAME),
            [{'x': 1}, {'x': 2}, {'x': 3}]
        )
        self.assertEqual(cursor.fetchall(), [[3]])

    def test_fetchone_no_data(self):
        cursor = presto.connect(host=_HOST).cursor()
        self.assertRaises(presto.ProgrammingError, lambda: cursor.fetchone())

    def test_fetchmany(self):
        cursor = presto.connect(host=_HOST).cursor()
        cursor.execute('select * from {} limit 15'.format(_BIG_TABLE_NAME))
        self.assertEqual(cursor.fetchmany(0), [])
        self.assertEqual(len(cursor.fetchmany(10)), 10)
        self.assertEqual(len(cursor.fetchmany(10)), 5)

    def test_arraysize(self):
        cursor = presto.connect(host=_HOST).cursor()
        cursor.arraysize = 5
        cursor.execute('select * from {} limit 20'.format(_BIG_TABLE_NAME))
        self.assertEqual(len(cursor.fetchmany()), 5)

    def test_slow_query(self):
        """Trigger the polling logic in fetchone()"""
        cursor = presto.connect(host=_HOST).cursor()
        cursor.execute('select count(*) from {}'.format(_BIG_TABLE_NAME))
        self.assertIsNotNone(cursor.fetchone())

    def test_no_params(self):
        cursor = presto.connect(host=_HOST).cursor()
        cursor.execute("select '%(x)s' from {}".format(_ONE_ROW_TABLE_NAME))
        self.assertEqual(cursor.fetchall(), [['%(x)s']])

    def test_noops(self):
        """The DB-API specification requires that certain actions exist, even though they might not
        be applicable."""
        # Wohoo inflating coverage stats!
        connection = presto.connect(host=_HOST)
        cursor = connection.cursor()
        self.assertEqual(cursor.rowcount, -1)
        cursor.setinputsizes([])
        cursor.setoutputsize(1, 'blah')
        cursor.close()
        connection.commit()
        connection.close()

    def test_escape(self):
        cursor = presto.connect(host=_HOST).cursor()
        cursor.execute(
            "select %d, %s from {}".format(_ONE_ROW_TABLE_NAME),
            (1, "';")
        )
        self.assertEqual(cursor.fetchall(), [[1, "';"]])
        cursor.execute(
            "select %(a)d, %(b)s from {}".format(_ONE_ROW_TABLE_NAME),
            {'a': 1, 'b': "';"}
        )
        self.assertEqual(cursor.fetchall(), [[1, "';"]])

    def test_invalid_params(self):
        cursor = presto.connect(host=_HOST).cursor()
        self.assertRaises(presto.ProgrammingError, lambda: cursor.execute('', 'hi'))
        self.assertRaises(presto.ProgrammingError, lambda: cursor.execute('', [{}]))

    @mock.patch('requests.post')
    def test_non_200(self, post):
        cursor = presto.connect(host=_HOST).cursor()
        post.return_value.status_code = 404
        self.assertRaises(presto.OperationalError,
            lambda: cursor.execute('show tables'))
