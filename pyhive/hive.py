"""DB-API implementation backed by HiveServer2 (Thrift API)

See http://www.python.org/dev/peps/pep-0249/

Many docstrings in this file are based on the PEP, which is in the public domain.
"""

from __future__ import absolute_import
from __future__ import unicode_literals
from TCLIService import TCLIService
from TCLIService import constants
from TCLIService import ttypes
from pyhive import common
from pyhive.common import DBAPITypeObject
# Make all exceptions visible in this module per DB-API
from pyhive.exc import *
import contextlib
import getpass
import logging
import sasl
import sys
import thrift.protocol.TBinaryProtocol
import thrift.transport.TSocket
import thrift_sasl

# PEP 249 module globals
apilevel = '2.0'
threadsafety = 2  # Threads may share the module and connections.
paramstyle = 'pyformat'  # Python extended format codes, e.g. ...WHERE name=%(name)s

_logger = logging.getLogger(__name__)


class HiveParamEscaper(common.ParamEscaper):
    def escape_string(self, item):
        # backslashes and single quotes need to be escaped
        # TODO verify against parser
        # Need to decode UTF-8 because of old sqlalchemy.
        # Newer SQLAlchemy checks dialect.supports_unicode_binds before encoding Unicode strings
        # as byte strings. The old version always encodes Unicode as byte strings, which breaks
        # string formatting here.
        if isinstance(item, str):
            item = item.decode('utf-8')
        return "'{}'".format(item
            .replace('\\', '\\\\')
            .replace("'", "\\'")
            .replace('\r', '\\r')
            .replace('\n', '\\n')
            .replace('\t', '\\t')
        )

_escaper = HiveParamEscaper()


def connect(*args, **kwargs):
    """Constructor for creating a connection to the database. See class :py:class:`Connection` for
    arguments.

    :returns: a :py:class:`Connection` object.
    """
    return Connection(*args, **kwargs)


class Connection(object):
    """Wraps a Thrift session"""

    def __init__(self, host, port=10000, username=None, database='default', configuration=None):
        socket = thrift.transport.TSocket.TSocket(host, port)
        username = username or getpass.getuser()
        configuration = configuration or {}

        def sasl_factory():
            sasl_client = sasl.Client()
            sasl_client.setAttr(b'username', username.encode('latin-1'))
            # Password doesn't matter in PLAIN mode, just needs to be nonempty.
            sasl_client.setAttr(b'password', b'x')
            sasl_client.init()
            return sasl_client

        # PLAIN corresponds to hive.server2.authentication=NONE in hive-site.xml
        self._transport = thrift_sasl.TSaslClientTransport(sasl_factory, b'PLAIN', socket)
        protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocol(self._transport)
        self._client = TCLIService.Client(protocol)

        try:
            self._transport.open()
            open_session_req = ttypes.TOpenSessionReq(
                client_protocol=ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1,
                configuration=configuration,
            )
            response = self._client.OpenSession(open_session_req)
            _check_status(response)
            assert(response.sessionHandle is not None), "Expected a session from OpenSession"
            self._sessionHandle = response.sessionHandle
            assert(response.serverProtocolVersion == ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1), \
                "Unable to handle protocol version {}".format(response.serverProtocolVersion)
            with contextlib.closing(self.cursor()) as cursor:
                cursor.execute('USE `{}`'.format(database))
        except:
            self._transport.close()
            raise

    def close(self):
        """Close the underlying session and Thrift transport"""
        req = ttypes.TCloseSessionReq(sessionHandle=self._sessionHandle)
        response = self._client.CloseSession(req)
        self._transport.close()
        _check_status(response)

    def commit(self):
        """Hive does not support transactions, so this does nothing."""
        pass

    def cursor(self):
        """Return a new :py:class:`Cursor` object using the connection."""
        return Cursor(self)

    @property
    def client(self):
        return self._client

    @property
    def sessionHandle(self):
        return self._sessionHandle

    def rollback(self):
        raise NotSupportedError("Hive does not have transactions")  # pragma: no cover


class Cursor(common.DBAPICursor):
    """These objects represent a database cursor, which is used to manage the context of a fetch
    operation.

    Cursors are not isolated, i.e., any changes done to the database by a cursor are immediately
    visible by other cursors or connections.
    """

    def __init__(self, connection):
        self._operationHandle = None
        super(Cursor, self).__init__()
        self._connection = connection

    def _reset_state(self):
        """Reset state about the previous query in preparation for running another query"""
        super(Cursor, self)._reset_state()
        self._description = None
        if self._operationHandle is not None:
            request = ttypes.TCloseOperationReq(self._operationHandle)
            try:
                response = self._connection.client.CloseOperation(request)
                _check_status(response)
            finally:
                self._operationHandle = None

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

        This attribute will be ``None`` for operations that do not return rows or if the cursor has
        not had an operation invoked via the :py:meth:`execute` method yet.

        The ``type_code`` can be interpreted by comparing it to the Type Objects specified in the
        section below.
        """
        if self._operationHandle is None or not self._operationHandle.hasResultSet:
            return None
        if self._description is None:
            req = ttypes.TGetResultSetMetadataReq(self._operationHandle)
            response = self._connection.client.GetResultSetMetadata(req)
            _check_status(response)
            columns = response.schema.columns
            self._description = []
            for col in columns:
                primary_type_entry = col.typeDesc.types[0]
                if primary_type_entry.primitiveEntry is None:
                    # All fancy stuff maps to string
                    type_code = ttypes.TTypeId._VALUES_TO_NAMES[ttypes.TTypeId.STRING_TYPE]
                else:
                    type_id = primary_type_entry.primitiveEntry.type
                    type_code = ttypes.TTypeId._VALUES_TO_NAMES[type_id]
                self._description.append((
                    col.columnName.decode('utf-8'), type_code.decode('utf-8'),
                    None, None, None, None, True
                ))
        return self._description

    def close(self):
        """Close the operation handle"""
        self._reset_state()

    def execute(self, operation, parameters=None):
        """Prepare and execute a database operation (query or command).

        Return values are not defined.
        """
        # Prepare statement
        if parameters is None:
            sql = operation
        else:
            sql = operation % _escaper.escape_args(parameters)

        self._reset_state()

        self._state = self._STATE_RUNNING
        _logger.info('%s', sql)

        req = ttypes.TExecuteStatementReq(self._connection.sessionHandle, sql.encode('utf-8'))
        _logger.debug(req)
        response = self._connection.client.ExecuteStatement(req)
        _check_status(response)
        self._operationHandle = response.operationHandle

    def _fetch_more(self):
        """Send another TFetchResultsReq and update state"""
        assert(self._state == self._STATE_RUNNING), "Should be running when in _fetch_more"
        assert(self._operationHandle is not None), "Should have an op handle in _fetch_more"
        if not self._operationHandle.hasResultSet:
            raise ProgrammingError("No result set")
        req = ttypes.TFetchResultsReq(
            operationHandle=self._operationHandle,
            orientation=ttypes.TFetchOrientation.FETCH_NEXT,
            maxRows=1000,
        )
        response = self._connection.client.FetchResults(req)
        _check_status(response)
        # response.hasMoreRows seems to always be False, so we instead check the number of rows
        #if not response.hasMoreRows:
        if not response.results.rows:
            self._state = self._STATE_FINISHED
        for row in response.results.rows:
            self._data.append([_unwrap_col_val(val) for val in row.colVals])


#
# Type Objects and Constructors
#


for type_id in constants.PRIMITIVE_TYPES:
    name = ttypes.TTypeId._VALUES_TO_NAMES[type_id]
    setattr(sys.modules[__name__], name, DBAPITypeObject([name]))


#
# Private utilities
#


def _unwrap_col_val(val):
    """Return the raw value from a TColumnValue instance."""
    for _, _, attr, _, _ in filter(None, ttypes.TColumnValue.thrift_spec):
        val_obj = getattr(val, attr)
        if val_obj:
            val = val_obj.value
            if isinstance(val, str):
                return val.decode('utf-8')
            else:
                return val
    raise DataError("Got empty column value {}".format(val))  # pragma: no cover


def _check_status(response):
    """Raise an OperationalError if the status is not success"""
    _logger.debug(response)
    if response.status.statusCode != ttypes.TStatusCode.SUCCESS_STATUS:
        raise OperationalError(response)
