"""
MySQLdb Cursors
---------------

This module implements the Cursor class. You should not try to
create Cursors direction; use connection.cursor() instead.

"""

import re
import sys
import weakref
from MySQLdb.converters import get_codec
from warnings import warn

INSERT_VALUES = re.compile(r"(?P<start>.+values\s*)"
                           r"(?P<values>\(((?<!\\)'[^\)]*?\)[^\)]*(?<!\\)?'|[^\(\)]|(?:\([^\)]*\)))+\))"
                           r"(?P<end>.*)", re.I)


class Cursor(object):

    """A base for Cursor classes. Useful attributes:

    description
        A tuple of DB API 7-tuples describing the columns in
        the last executed query; see PEP-249 for details.

    arraysize
        default number of rows fetchmany() will fetch

    """

    from MySQLdb.exceptions import MySQLError, Warning, Error, InterfaceError, \
         DatabaseError, DataError, OperationalError, IntegrityError, \
         InternalError, ProgrammingError, NotSupportedError

    _defer_warnings = False
    _fetch_type = None

    def __init__(self, connection, encoders, decoders, row_formatter):
        self.connection = weakref.proxy(connection)
        self.description_flags = None
        self.rowcount = -1
        self.arraysize = 1
        self._executed = None
        self.lastrowid = None
        self.messages = []
        self.errorhandler = connection.errorhandler
        self._result = None
        self._pending_results = []
        self._warnings = 0
        self._info = None
        self.rownumber = None
        self.maxrows = 0
        self.encoders = encoders
        self.decoders = decoders
        self._row_decoders = ()
        self.row_formatter = row_formatter
        self.use_result = False

    @property
    def description(self):
        if self._result:
            return self._result.description
        return None

    def _flush(self):
        """_flush() reads to the end of the current result set, buffering what
        it can, and then releases the result set."""
        if self._result:
            self._result.flush()
            self._result = None
        db = self._get_db()
        while db.next_result():
            result = Result(self)
            result.flush()
            self._pending_results.append(result)

    def __del__(self):
        self.close()
        self.errorhandler = None
        self._result = None
        del self._pending_results[:]

    def _clear(self):
        if self._result:
            self._result.clear()
            self._result = None
        for result in self._pending_results:
            result.clear()
        del self._pending_results[:]
        db = self._get_db()
        while db.next_result():
            result = db.get_result(True)
            if result:
                result.clear()
        del self.messages[:]

    def close(self):
        """Close the cursor. No further queries will be possible."""
        if not self.connection:
            return

        self._flush()
        try:
            while self.nextset():
                pass
        except:
            pass
        self.connection = None

    def _check_executed(self):
        """Ensure that .execute() has been called."""
        if not self._executed:
            self.errorhandler(self, self.ProgrammingError, "execute() first")

    def _warning_check(self):
        """Check for warnings, and report via the warnings module."""
        from warnings import warn
        if self._warnings:
            warnings = self._get_db()._show_warnings()
            if warnings:
                # This is done in two loops in case
                # Warnings are set to raise exceptions.
                for warning in warnings:
                    self.messages.append((self.Warning, warning))
                for warning in warnings:
                    warn(warning[-1], self.Warning, 3)
            elif self._info:
                self.messages.append((self.Warning, self._info))
                warn(self._info, self.Warning, 3)

    def nextset(self):
        """Advance to the next result set.

        Returns False if there are no more result sets.
        """
        db = self._get_db()
        self._result.clear()
        self._result = None
        if self._pending_results:
            self._result = self._pending_results[0]
            del self._pending_results[0]
            return True
        if db.next_result():
            self._result = Result(self)
            return True
        return False

    def setinputsizes(self, *args):
        """Does nothing, required by DB API."""

    def setoutputsizes(self, *args):
        """Does nothing, required by DB API."""

    def _get_db(self):
        """Get the database connection.

        Raises ProgrammingError if the connection has been closed."""
        if not self.connection:
            self.errorhandler(self, self.ProgrammingError, "cursor closed")
        return self.connection._db

    def execute(self, query, args=None):
        """Execute a query.

        query -- string, query to execute on server
        args -- optional sequence or mapping, parameters to use with query.

        Note: If args is a sequence, then %s must be used as the
        parameter placeholder in the query. If a mapping is used,
        %(key)s must be used as the placeholder.

        Returns long integer rows affected, if any

        """
        db = self._get_db()
        self._clear()
        charset = db.character_set_name()
        if isinstance(query, unicode):
            query = query.encode(charset)
        try:
            if args is not None:
                query = query % tuple(( get_codec(a, self.encoders)(db, a) for a in args ))
            self._query(query)
        except TypeError, msg:
            if msg.args[0] in ("not enough arguments for format string",
                               "not all arguments converted"):
                self.messages.append((self.ProgrammingError, msg.args[0]))
                self.errorhandler(self, self.ProgrammingError, msg.args[0])
            else:
                self.messages.append((TypeError, msg))
                self.errorhandler(self, TypeError, msg)
        except:
            exc, value, traceback = sys.exc_info()
            del traceback
            self.messages.append((exc, value))
            self.errorhandler(self, exc, value)

        if not self._defer_warnings:
            self._warning_check()
        return None

    def executemany(self, query, args):
        """Execute a multi-row query.

        query

            string, query to execute on server

        args

            Sequence of sequences or mappings, parameters to use with
            query.

        Returns long integer rows affected, if any.

        This method improves performance on multiple-row INSERT and
        REPLACE. Otherwise it is equivalent to looping over args with
        execute().

        """
        db = self._get_db()
        self._clear()
        if not args:
            return
        charset = self.connection.character_set_name()
        if isinstance(query, unicode):
            query = query.encode(charset)
        matched = INSERT_VALUES.match(query)
        if not matched:
            rowcount = 0
            for row in args:
                self.execute(query, row)
                rowcount += self.rowcount
            self.rowcount = rowcount
            return

        start = matched.group('start')
        values = matched.group('values')
        end = matched.group('end')

        try:
            sql_params = ( values % tuple(( get_codec(a, self.encoders)(db, a) for a in row )) for row in args )
            multirow_query = '\n'.join([start, ',\n'.join(sql_params), end])
            self._query(multirow_query)

        except TypeError, msg:
            if msg.args[0] in ("not enough arguments for format string",
                               "not all arguments converted"):
                self.messages.append((self.ProgrammingError, msg.args[0]))
                self.errorhandler(self, self.ProgrammingError, msg.args[0])
            else:
                self.messages.append((TypeError, msg))
                self.errorhandler(self, TypeError, msg)
        except:
            exc, value, traceback = sys.exc_info()
            del traceback
            self.errorhandler(self, exc, value)

        if not self._defer_warnings:
            self._warning_check()
        return None

    def callproc(self, procname, args=()):
        """Execute stored procedure procname with args

        procname
            string, name of procedure to execute on server

        args
            Sequence of parameters to use with procedure

        Returns the original args.

        Compatibility warning: PEP-249 specifies that any modified
        parameters must be returned. This is currently impossible
        as they are only available by storing them in a server
        variable and then retrieved by a query. Since stored
        procedures return zero or more result sets, there is no
        reliable way to get at OUT or INOUT parameters via callproc.
        The server variables are named @_procname_n, where procname
        is the parameter above and n is the position of the parameter
        (from zero). Once all result sets generated by the procedure
        have been fetched, you can issue a SELECT @_procname_0, ...
        query using .execute() to get any OUT or INOUT values.

        Compatibility warning: The act of calling a stored procedure
        itself creates an empty result set. This appears after any
        result sets generated by the procedure. This is non-standard
        behavior with respect to the DB-API. Be sure to use nextset()
        to advance through all result sets; otherwise you may get
        disconnected.
        """

        db = self._get_db()
        charset = self.connection.character_set_name()
        for index, arg in enumerate(args):
            query = "SET @_%s_%d=%s" % (procname, index,
                                        self.connection.literal(arg))
            if isinstance(query, unicode):
                query = query.encode(charset)
            self._query(query)
            self.nextset()

        query = "CALL %s(%s)" % (procname,
                                 ','.join(['@_%s_%d' % (procname, i)
                                           for i in range(len(args))]))
        if isinstance(query, unicode):
            query = query.encode(charset)
        self._query(query)
        if not self._defer_warnings:
            self._warning_check()
        return args

    def __iter__(self):
        return iter(self.fetchone, None)

    def _query(self, query):
        """Low-level; executes query, gets result, sets up decoders."""
        connection = self._get_db()
        self._flush()
        self._executed = query
        connection.query(query)
        self._result = Result(self)

    def fetchone(self):
        """Fetches a single row from the cursor. None indicates that
        no more rows are available."""
        self._check_executed()
        if not self._result:
            return None
        return self._result.fetchone()

    def fetchmany(self, size=None):
        """Fetch up to size rows from the cursor. Result set may be smaller
        than size. If size is not defined, cursor.arraysize is used."""
        self._check_executed()
        if not self._result:
            return []
        if size is None:
            size = self.arraysize
        return self._result.fetchmany(size)

    def fetchall(self):
        """Fetches all available rows from the cursor."""
        self._check_executed()
        if not self._result:
            return []
        return self._result.fetchall()

    def scroll(self, value, mode='relative'):
        """Scroll the cursor in the result set to a new position according
        to mode.

        If mode is 'relative' (default), value is taken as offset to
        the current position in the result set, if set to 'absolute',
        value states an absolute target position."""
        self._check_executed()
        if mode == 'relative':
            row = self.rownumber + value
        elif mode == 'absolute':
            row = value
        else:
            self.errorhandler(self, self.ProgrammingError,
                              "unknown scroll mode %s" % `mode`)
        if row < 0 or row >= len(self._rows):
            self.errorhandler(self, IndexError, "out of range")
        self.rownumber = row


class Result(object):

    def __init__(self, cursor):
        self.cursor = cursor
        db = cursor._get_db()
        result = db.get_result(cursor.use_result)
        self.result = result
        decoders = cursor.decoders
        self.row_formatter = cursor.row_formatter
        self.max_buffer = 1000
        self.rows = []
        self.row_start = 0
        self.rows_read = 0
        self.row_index = 0
        self.lastrowid = db.insert_id()
        self.warning_count = db.warning_count()
        self.info = db.info()
        self.rowcount = -1
        self.description = None
        self.field_flags = ()
        self.row_decoders = ()

        if result:
            self.description = result.describe()
            self.field_flags = result.field_flags()
            self.row_decoders = tuple(( get_codec(field, decoders) for field in result.fields ))
            if not cursor.use_result:
                self.rowcount = db.affected_rows()
                self.flush()

    def flush(self):
        if self.result:
            self.rows.extend([ self.row_formatter(self.row_decoders, row) for row in self.result ])
            self.result.clear()
            self.result = None

    def clear(self):
        if self.result:
            self.result.clear()
            self.result = None

    def fetchone(self):
        if self.result:
            while self.row_index >= len(self.rows):
                row = self.result.fetch_row()
                if row is None:
                    return row
                self.rows.append(self.row_formatter(self.row_decoders, row))
        if self.row_index >= len(self.rows):
            return None
        row = self.rows[self.row_index]
        self.row_index += 1
        return row

    def __iter__(self): return self

    def next(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def fetchmany(self, size):
        """Fetch up to size rows from the cursor. Result set may be smaller
        than size. If size is not defined, cursor.arraysize is used."""
        row_end = self.row_index + size
        if self.result:
            while self.row_index >= len(self.rows):
                row = self.result.fetch_row()
                if row is None:
                    break
                self.rows.append(self.row_formatter(self.row_decoders, row))
        if self.row_index >= len(self.rows):
            return []
        if row_end >= len(self.rows):
            row_end = len(self.rows)
        rows = self.rows[self.row_index:row_end]
        self.row_index = row_end
        return rows

    def fetchall(self):
        if self.result:
            self.flush()
        rows = self.rows[self.row_index:]
        self.row_index = len(self.rows)
        return rows

    def warning_check(self):
        """Check for warnings, and report via the warnings module."""
        if self.warning_count:
            cursor = self.cursor
            warnings = cursor._get_db()._show_warnings()
            if warnings:
                # This is done in two loops in case
                # Warnings are set to raise exceptions.
                for warning in warnings:
                    cursor.warnings.append((self.Warning, warning))
                for warning in warnings:
                    warn(warning[-1], self.Warning, 3)
            elif self._info:
                cursor.messages.append((self.Warning, self._info))
                warn(self._info, self.Warning, 3)


