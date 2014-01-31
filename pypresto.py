"""DB API implementation backed by Presto

See http://www.python.org/dev/peps/pep-0249/

Many docstrings in this file are based on the PEP, which is in the public domain.
"""

import collections
import exceptions
import getpass
import logging
import requests
import time
import urlparse


# PEP 249 module globals
apilevel = '2.0'
threadsafety = 2  # Threads may share the module and connections.
paramstyle = 'pyformat'  # Python extended format codes, e.g. ...WHERE name=%(name)s

_logger = logging.getLogger(__name__)


def connect(**kwargs):
    """Constructor for creating a connection to the database. See class Connection for arguments.

    Returns a Connection object.
    """
    return Connection(**kwargs)


class Connection(object):
    """Presto does not have a notion of a persistent connection.

    Thus, these objects are small stateless factories for cursors, which do all the real work.
    """

    def __init__(self, **kwargs):
        self._params = kwargs

    def close(self):
        """Presto does not have anything to close"""
        # TODO cancel outstanding queries?
        pass

    def commit(self):
        """Presto does not support transactions"""
        pass

    def cursor(self):
        """Return a new Cursor object using the connection."""
        return Cursor(**self._params)


class Cursor(object):
    """These objects represent a database cursor, which is used to manage the context of a fetch
    operation.

    Cursors are not isolated, i.e., any changes done to the database by a cursor are immediately
    visible by other cursors or connections.
    """
    _STATE_NONE = 0
    _STATE_RUNNING = 1
    _STATE_FINISHED = 2

    def __init__(self, host, port='8080', user=None, catalog='hive', schema='default',
                 poll_interval=1, source='pypresto'):
        """
        :param host: hostname to connect to, e.g. ``presto.example.com``
        :param port: int -- port, defaults to 8080
        :param user: string -- defaults to system user name
        :param catalog: string -- defaults to ``hive``
        :param schema: string -- defaults to ``default``
        :param poll_interval: int -- how often to ask the Presto REST interface for a progress
            update, defaults to a second
        :param source: string -- arbitrary identifier (shows up in the Presto monitoring page)
        """
        # Config
        self._host = host
        self._port = port
        self._user = user or getpass.getuser()
        self._catalog = catalog
        self._schema = schema
        self._arraysize = 1
        self._poll_interval = poll_interval
        self._source = source

        self._reset_state()

    def _reset_state(self):
        """Reset state about the previous query in preparation for running another query"""
        # State to return as part of DBAPI
        self._rownumber = 0

        # Internal helper state
        self._state = self._STATE_NONE
        self._nextUri = None
        self._data = collections.deque()
        self._columns = None

    @property
    def description(self):
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result column:

         - name
         - type_code
         - display_size (None in current implementation)
         - internal_size (None in current implementation)
         - precision (None in current implementation)
         - scale (None in current implementation)
         - null_ok (always True in current implementation)

        The type_code can be interpreted by comparing it to the Type Objects specified in the section below.
        """
        if self._columns is None:
            return None
        return [
            # name, type_code, display_size, internal_size, precision, scale, null_ok
            (col['name'], col['type'], None, None, None, None, True)
            for col in self._columns
        ]

    @property
    def rowcount(self):
        """Presto does not support rowcount"""
        return -1

    def close(self):
        """Presto does not have anything to close"""
        pass

    def execute(self, operation, parameters=None):
        """Prepare and execute a database operation (query or command).

        Return values are not defined.
        """
        if self._state == self._STATE_RUNNING:
            raise ProgrammingError("Already running a query")

        headers = {
            'X-Presto-Catalog': self._catalog,
            'X-Presto-Schema': self._schema,
            'X-Presto-Source': self._source,
            'X-Presto-User': self._user,
        }

        # Prepare statement
        if parameters is None:
            sql = operation
        else:
            sql = operation % _escape_args(parameters)

        self._reset_state()

        self._state = self._STATE_RUNNING
        url = urlparse.urlunparse((
            'http', self._host + ':' + self._port, '/v1/statement', None, None, None))
        _logger.debug("Query %s", sql)
        _logger.debug("Headers %s", headers)
        response = requests.post(url, data=sql, headers=headers)
        self._process_response(response)

    def _fetch_more(self):
        """Fetch the next URI and udpate state"""
        self._process_response(requests.get(self._nextUri))

    def _process_response(self, response):
        """Given the JSON response from Presto's REST API, update the internal state with the next
        URI and any data from the response
        """
        # TODO handle HTTP 503
        if response.status_code != requests.codes.ok:
            fmt = "Unexpected status code {}\n{}"
            raise OperationalError(fmt.format(response.status_code, response.content))
        response_json = response.json()
        _logger.debug("Got response %s", response_json)
        assert self._state == self._STATE_RUNNING, 'Should be running if processing response'
        self._nextUri = response_json.get('nextUri')
        self._columns = response_json.get('columns')
        self._data += response_json.get('data', [])
        if 'nextUri' not in response_json:
            self._state = self._STATE_FINISHED
        if 'error' in response_json:
            assert not self._nextUri, 'Should not have nextUri if failed'
            raise DatabaseError(response_json['error'])

    def executemany(self, operation, seq_of_parameters):
        """Prepare a database operation (query or command) and then execute it against all parameter
        sequences or mappings found in the sequence seq_of_parameters.

        Only the final result set is retained.

        Return values are not defined.
        """
        for parameters in seq_of_parameters[:-1]:
            self.execute(operation, parameters)
            while self._state != self._STATE_FINISHED:
                self._fetch_more()
        self.execute(operation, seq_of_parameters[-1])

    def fetchone(self):
        """Fetch the next row of a query result set, returning a single sequence, or None when no
        more data is available.

        An Error (or subclass) exception is raised if the previous call to execute() did not
        produce any result set or no call was issued yet.
        """
        if self._state == self._STATE_NONE:
            raise ProgrammingError('No query yet')
        # Note: all Presto statements produce a result set
        # The CREATE TABLE statement produces a single bigint called 'rows'

        # Sleep until we're done or we have some data to return
        while not self._data and self._state != self._STATE_FINISHED:
            self._fetch_more()
            if not self._data and self._state != self._STATE_FINISHED:
                time.sleep(self._poll_interval)

        if not self._data:
            return None
        else:
            self._rownumber += 1
            return self._data.popleft()

    def fetchmany(self, size=None):
        """Fetch the next set of rows of a query result, returning a sequence of sequences (e.g. a
        list of tuples). An empty sequence is returned when no more rows are available.

        The number of rows to fetch per call is specified by the parameter. If it is not given, the
        cursor's arraysize determines the number of rows to be fetched. The method should try to
        fetch as many rows as indicated by the size parameter. If this is not possible due to the
        specified number of rows not being available, fewer rows may be returned.

        An Error (or subclass) exception is raised if the previous call to .execute*() did not
        produce any result set or no call was issued yet.
        """
        if size is None:
            size = self.arraysize
        result = []
        for _ in xrange(size):
            one = self.fetchone()
            if one is None:
                break
            else:
                result.append(one)
        return result

    def fetchall(self):
        """Fetch all (remaining) rows of a query result, returning them as a sequence of sequences
        (e.g. a list of tuples).

        An Error (or subclass) exception is raised if the previous call to .execute*() did not
        produce any result set or no call was issued yet.
        """
        result = []
        while True:
            one = self.fetchone()
            if one is None:
                break
            else:
                result.append(one)
        return result

    @property
    def arraysize(self):
        """This read/write attribute specifies the number of rows to fetch at a time with
        ``.fetchmany()``. It defaults to 1 meaning to fetch a single row at a time.

        In our current implementation this parameter has no effect on actual fetching.
        """
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        self._arraysize = value

    def setinputsizes(self, sizes):
        """Does nothing for Presto"""
        pass

    def setoutputsize(self, size, column=None):
        """Does nothing for Presto"""
        pass

    #
    # Optional DB API Extensions
    #

    @property
    def rownumber(self):
        """This read-only attribute should provide the current 0-based index of the cursor in the
        result set.

        The index can be seen as index of the cursor in a sequence (the result set). The next fetch
        operation will fetch the row indexed by .rownumber in that sequence.
        """
        return self._rownumber

    def next(self):
        """Return the next row from the currently executing SQL statement using the same semantics
        as ``.fetchone()``. A StopIteration exception is raised when the result set is exhausted.
        """
        one = self.fetchone()
        if one is None:
            raise StopIteration
        else:
            return one

    def __iter__(self):
        """Return self to make cursors compatible to the iteration protocol."""
        return self

#
# Exceptions
#


class Error(exceptions.StandardError):
    """Exception that is the base class of all other error exceptions.

    You can use this to catch all errors with one single except statement.
    """
    pass


class Warning(exceptions.StandardError):
    """Exception raised for important warnings like data truncations while inserting, etc."""
    pass


class InterfaceError(Error):
    """Exception raised for errors that are related to the database interface rather than the
    database itself.
    """
    pass


class DatabaseError(Error):
    """Exception raised for errors that are related to the database."""
    pass


class InternalError(DatabaseError):
    """Exception raised when the database encounters an internal error, e.g. the cursor is not valid
    anymore, the transaction is out of sync, etc."""
    pass


class OperationalError(DatabaseError):
    """Exception raised for errors that are related to the database's operation and not necessarily
    under the control of the programmer, e.g. an unexpected disconnect occurs, the data source name
    is not found, a transaction could not be processed, a memory allocation error occurred during
    processing, etc.
    """
    pass


class ProgrammingError(DatabaseError):
    """Exception raised for programming errors, e.g. table not found or already exists, syntax error
    in the SQL statement, wrong number of parameters specified, etc.
    """
    pass


class DataError(DatabaseError):
    """Exception raised for errors that are due to problems with the processed data like division by
    zero, numeric value out of range, etc.
    """
    pass


class NotSupportedError(DatabaseError):
    """Exception raised in case a method or database API was used which is not supported by the
    database, e.g. requesting a .rollback() on a connection that does not support transaction or
    has transactions turned off.
    """
    pass


#
# Type Objects and Constructors
#


class DBAPITypeObject(object):
    # Taken from http://www.python.org/dev/peps/pep-0249/#implementation-hints
    def __init__(self, *values):
        self.values = values

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1

# See types in presto-main/src/main/java/com/facebook/presto/tuple/TupleInfo.java
FIXED_INT_64 = DBAPITypeObject(['bigint'])
VARIABLE_BINARY = DBAPITypeObject(['varchar'])
DOUBLE = DBAPITypeObject(['double'])
BOOLEAN = DBAPITypeObject(['boolean'])


#
# Private utilities
#
def _escape_args(parameters):
    if isinstance(parameters, dict):
        return {k: _escape_item(v) for k, v in parameters.iteritems()}
    elif isinstance(parameters, (list, tuple)):
        return tuple(_escape_item(x) for x in parameters)
    else:
        raise ProgrammingError("Unsupported param format: {}".format(parameters))


def _escape_item(item):
    if isinstance(item, (int, long, float)):
        return item
    elif isinstance(item, basestring):
        # TODO is this good enough?
        return "'{}'".format(item.replace("'", "''"))
    else:
        raise ProgrammingError("Unsupported object {}".format(item))
