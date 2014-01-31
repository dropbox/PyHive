"""PyPresto integration tests.

These rely on having a Presto+Hadoop cluster set up. They also require a table called one_row
"""
import pypresto
import unittest

_HOST = 'localhost'
_ONE_ROW_TABLE_NAME = 'one_row'
_BIG_TABLE_NAME = 'user'


class TestPyPresto(unittest.TestCase):
    def test_fetchone(self):
        cursor = pypresto.connect(host=_HOST).cursor()
        cursor.execute('select 1 from {}'.format(_ONE_ROW_TABLE_NAME))
        self.assertEqual(cursor.rownumber, 0)
        self.assertEqual(cursor.fetchone(), [1])
        self.assertEqual(cursor.rownumber, 1)
        self.assertIsNone(cursor.fetchone())

    def test_fetchall(self):
        cursor = pypresto.connect(host=_HOST).cursor()
        cursor.execute('select count(*) from {}'.format(_ONE_ROW_TABLE_NAME))
        self.assertEqual(cursor.fetchall(), [[1]])

    def test_iterator(self):
        cursor = pypresto.connect(host=_HOST).cursor()
        cursor.execute('select count(*) from {}'.format(_ONE_ROW_TABLE_NAME))
        self.assertEqual(list(cursor), [[1]])
        self.assertRaises(StopIteration, lambda: cursor.next())

    def test_description(self):
        cursor = pypresto.connect(host=_HOST).cursor()
        cursor.execute('select 1 as foobar from {}'.format(_ONE_ROW_TABLE_NAME))
        # Not a valid assumption in general. We want to check the initial period where the columns
        # are not yet available.
        self.assertIsNone(cursor.description)
        cursor.fetchone()
        self.assertEqual(cursor.description, [('foobar', 'bigint', None, None, None, None, True)])

    def test_bad_query(self):
        cursor = pypresto.connect(host=_HOST).cursor()

        def run():
            cursor.execute('select does_not_exist from {}'.format(_ONE_ROW_TABLE_NAME))
            cursor.fetchone()
        self.assertRaises(pypresto.DatabaseError, run)

    def test_concurrent_execution(self):
        cursor = pypresto.connect(host=_HOST).cursor()
        cursor.execute('select count(*) from {}'.format(_ONE_ROW_TABLE_NAME))
        self.assertRaises(pypresto.ProgrammingError,
            lambda: cursor.execute('select count(*) from {}'.format(_ONE_ROW_TABLE_NAME)))

    def test_executemany(self):
        cursor = pypresto.connect(host=_HOST).cursor()
        cursor.executemany(
            'select %(x)d from {}'.format(_ONE_ROW_TABLE_NAME),
            [{'x': 1}, {'x': 2}, {'x': 3}]
        )
        self.assertEqual(cursor.fetchall(), [[3]])

    def test_fetchone_no_data(self):
        cursor = pypresto.connect(host=_HOST).cursor()
        self.assertRaises(pypresto.ProgrammingError, lambda: cursor.fetchone())

    def test_fetchmany(self):
        cursor = pypresto.connect(host=_HOST).cursor()
        cursor.execute('select * from {} limit 15'.format(_BIG_TABLE_NAME))
        self.assertEqual(cursor.fetchmany(0), [])
        self.assertEqual(len(cursor.fetchmany(10)), 10)
        self.assertEqual(len(cursor.fetchmany(10)), 5)

    def test_arraysize(self):
        cursor = pypresto.connect(host=_HOST).cursor()
        cursor.arraysize = 5
        cursor.execute('select * from {} limit 20'.format(_BIG_TABLE_NAME))
        self.assertEqual(len(cursor.fetchmany()), 5)

    def test_slow_query(self):
        """Trigger the polling logic in fetchone()"""
        cursor = pypresto.connect(host=_HOST).cursor()
        cursor.execute('select count(*) from {}'.format(_BIG_TABLE_NAME))
        self.assertIsNotNone(cursor.fetchone())

    def test_no_params(self):
        cursor = pypresto.connect(host=_HOST).cursor()
        cursor.execute("select '%(x)s' from {}".format(_ONE_ROW_TABLE_NAME))
        self.assertEqual(cursor.fetchall(), [['%(x)s']])

    def test_noops(self):
        """The DB-API specification requires that certain actions exist, even though they might not
        be applicable."""
        # Wohoo inflating coverage stats!
        connection = pypresto.connect(host=_HOST)
        cursor = connection.cursor()
        self.assertEqual(cursor.rowcount, -1)
        cursor.setinputsizes([])
        cursor.setoutputsize(1, 'blah')
        cursor.close()
        connection.commit()
        connection.close()
