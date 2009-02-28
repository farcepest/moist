"""
MySQLdb Cursors
---------------

This module implements Cursors of various types for MySQLdb. By
default, MySQLdb uses the Cursor class.

"""

__revision__ = "$Revision$"[11:-2]
__author__ = "$Author$"[9:-2]

import re
import sys
import weakref

INSERT_VALUES = re.compile(r"\svalues\s*"
                           r"(\(((?<!\\)'[^\)]*?\)[^\)]*(?<!\\)?'"
                           r"|[^\(\)]|"
                           r"(?:\([^\)]*\))"
                           r")+\))")

class BaseCursor(object):
    
    """A base for Cursor classes. Useful attributes:
    
    description
        A tuple of DB API 7-tuples describing the columns in
        the last executed query; see PEP-249 for details.

    description_flags
        Tuple of column flags for last query, one entry per column
        in the result set. Values correspond to those in
        MySQLdb.constants.FLAG. See MySQL documentation (C API)
        for more information. Non-standard extension.
    
    arraysize
        default number of rows fetchmany() will fetch

    """

    from MySQLdb.exceptions import MySQLError, Warning, Error, InterfaceError, \
         DatabaseError, DataError, OperationalError, IntegrityError, \
         InternalError, ProgrammingError, NotSupportedError

    _defer_warnings = False
    _fetch_type = None

    def __init__(self, connection):
        self.connection = weakref.proxy(connection)
        self.description = None
        self.description_flags = None
        self.rowcount = -1
        self.arraysize = 1
        self._executed = None
        self.lastrowid = None
        self.messages = []
        self.errorhandler = connection.errorhandler
        self._result = None
        self._warnings = 0
        self._info = None
        self.rownumber = None

    def __del__(self):
        self.close()
        self.errorhandler = None
        self._result = None

    def close(self):
        """Close the cursor. No further queries will be possible."""
        if not self.connection:
            return
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
            warnings = self._get_db().show_warnings()
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

        Returns None if there are no more result sets.
        """
        if self._executed:
            self.fetchall()
        del self.messages[:]
        
        connection = self._get_db()
        num_rows = connection.next_result()
        if num_rows == -1:
            return None
        self._do_get_result()
        self._post_get_result()
        self._warning_check()
        return True

    def _post_get_result(self):
        """Stub to be overridden by MixIn."""
        
    def _get_result(self):
        """Stub to be overridden by MixIn."""
        return []
    
    def _do_get_result(self):
        """Get the result from the last query."""
        from MySQLdb.converters import lookup_converter
        connection = self._get_db()
        self._result = self._get_result()
        if self._result:
            self.sql_to_python = [ 
                lookup_converter(self, f)
                for f in self._result.fields()
            ]
        else:
            self.sql_to_python = []
        self.rowcount = connection.affected_rows()
        self.rownumber = 0
        self.description = self._result and self._result.describe() or None
        self.description_flags = self._result and self._result.field_flags() or None
        self.lastrowid = connection.insert_id()
        self._warnings = connection.warning_count()
        self._info = connection.info()
    
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
        del self.messages[:]
        db = self._get_db()
        charset = db.character_set_name()
        if isinstance(query, unicode):
            query = query.encode(charset)
        if args is not None:
            query = query % self.connection.literal(args)
        try:
            result = self._query(query)
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
            
        self._executed = query
        if not self._defer_warnings:
            self._warning_check()
        return result

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
        del self.messages[:]
        db = self._get_db()
        if not args:
            return
        charset = self.connection.character_set_name()
        if isinstance(query, unicode):
            query = query.encode(charset)
        matched = INSERT_VALUES.match(query)
        if not matched:
            self.rowcount = sum([ self.execute(query, arg) for arg in args ])
            return self.rowcount
        
        start = matched.group('start')
        end = matched.group('end')
        values = matched.group('values')
 
        try:
            sql_params = [ values % self.connection.literal(arg) for arg in args ]
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
        self.rowcount = int(self._query(
            '\n'.join([start, ',\n'.join(sql_params), end,
        ])))
        if not self._defer_warnings:
            self._warning_check()
        return self.rowcount
    
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
        self._executed = query
        if not self._defer_warnings:
            self._warning_check()
        return args
    
    def _do_query(self, query):
        """Low-levey query wrapper. Overridden by MixIns."""
        connection = self._get_db()
        self._executed = query
        connection.query(query)
        self._do_get_result()
        return self.rowcount

    def _query(self, query):
        """Hook for _do_query."""
        return self._do_query(query)
    
    def _fetch_row(self, size=1):
        """Low-level fetch_row wrapper."""
        if not self._result:
            return ()
        # unfortunately it is necessary to wrap these generators up as tuples
        # as the rows are expected to be subscriptable.
        return tuple(
            ( 
                tuple( ( f(x) for f, x in zip(self.sql_to_python, row) ) )
                for row in self._result.fetch_row(size, self._fetch_type)
            )
        )

    def __iter__(self):
        return iter(self.fetchone, None)

    def fetchone(self):
        """Stub to be overridden by a MixIn."""
        return None
    
    def fetchall(self):
        """Stub to be overridden by a MixIn."""
        return []
    

class CursorStoreResultMixIn(object):

    """This is a MixIn class which causes the entire result set to be
    stored on the client side, i.e. it uses mysql_store_result(). If the
    result set can be very large, consider adding a LIMIT clause to your
    query, or using CursorUseResultMixIn instead."""

    def _get_result(self):
        """Low-level; uses mysql_store_result()"""
        return self._get_db().store_result()

    def _query(self, query):
        """Low-level; executes query, gets result, and returns rowcount."""
        rowcount = self._do_query(query)
        self._post_get_result()
        return rowcount

    def _post_get_result(self):
        """Low-level"""
        self._rows = self._fetch_row(0)
        self._result = None

    def fetchone(self):
        """Fetches a single row from the cursor. None indicates that
        no more rows are available."""
        self._check_executed()
        if self.rownumber >= len(self._rows):
            return None
        result = self._rows[self.rownumber]
        self.rownumber += 1
        return result

    def fetchmany(self, size=None):
        """Fetch up to size rows from the cursor. Result set may be smaller
        than size. If size is not defined, cursor.arraysize is used."""
        self._check_executed()
        end = self.rownumber + (size or self.arraysize)
        result = self._rows[self.rownumber:end]
        self.rownumber = min(end, len(self._rows))
        return result

    def fetchall(self):
        """Fetchs all available rows from the cursor."""
        self._check_executed()
        if self.rownumber:
            result = self._rows[self.rownumber:]
        else:
            result = self._rows
        self.rownumber = len(self._rows)
        return result
    
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

    def __iter__(self):
        self._check_executed()
        result = self.rownumber and self._rows[self.rownumber:] or self._rows
        return iter(result)
    

class CursorUseResultMixIn(object):

    """This is a MixIn class which causes the result set to be stored
    in the server and sent row-by-row to client side, i.e. it uses
    mysql_use_result(). You MUST retrieve the entire result set and
    close() the cursor before additional queries can be peformed on
    the connection."""

    _defer_warnings = True
    
    def _get_result(self):
        """Low-level; calls mysql_use_result()"""
        return self._get_db().use_result()

    def fetchone(self):
        """Fetches a single row from the cursor."""
        self._check_executed()
        rows = self._fetch_row(1)
        if not rows:
            self._warning_check()
            return None
        self.rownumber = self.rownumber + 1
        return rows[0]
             
    def fetchmany(self, size=None):
        """Fetch up to size rows from the cursor. Result set may be smaller
        than size. If size is not defined, cursor.arraysize is used."""
        self._check_executed()
        rows = self._fetch_row(size or self.arraysize)
        self.rownumber = self.rownumber + len(rows)
        if not rows:
            self._warning_check()
        return rows
         
    def fetchall(self):
        """Fetchs all available rows from the cursor."""
        self._check_executed()
        rows = self._fetch_row(0)
        self.rownumber = self.rownumber + len(rows)
        self._warning_check()
        return rows

    def __iter__(self):
        return self

    def next(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row
    

class CursorTupleRowsMixIn(object):

    """This is a MixIn class that causes all rows to be returned as tuples,
    which is the standard form required by DB API."""

    _fetch_type = 0


class CursorDictRowsMixIn(object):

    """This is a MixIn class that causes all rows to be returned as
    dictionaries. This is a non-standard feature."""

    _fetch_type = 1


class Cursor(CursorStoreResultMixIn, CursorTupleRowsMixIn,
             BaseCursor):

    """This is the standard Cursor class that returns rows as tuples
    and stores the result set in the client."""


class DictCursor(CursorStoreResultMixIn, CursorDictRowsMixIn,
                 BaseCursor):

    """This is a Cursor class that returns rows as dictionaries and
    stores the result set in the client."""
   

class SSCursor(CursorUseResultMixIn, CursorTupleRowsMixIn,
               BaseCursor):

    """This is a Cursor class that returns rows as tuples and stores
    the result set in the server."""


class SSDictCursor(CursorUseResultMixIn, CursorDictRowsMixIn,
                   BaseCursor):

    """This is a Cursor class that returns rows as dictionaries and
    stores the result set in the server."""


