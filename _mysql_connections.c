#include "_mysql.h"

static int
_mysql_ConnectionObject_Initialize(
	_mysql_ConnectionObject *self,
	PyObject *args,
	PyObject *kwargs)
{
	MYSQL *conn = NULL;
	PyObject *conv = NULL;
	PyObject *ssl = NULL;
#if HAVE_OPENSSL
	char *key = NULL, *cert = NULL, *ca = NULL,
		*capath = NULL, *cipher = NULL;
#endif
	char *host = NULL, *user = NULL, *passwd = NULL,
		*db = NULL, *unix_socket = NULL;
	unsigned int port = 0;
	unsigned int client_flag = 0;
	static char *kwlist[] = { "host", "user", "passwd", "db", "port",
				  "unix_socket", "conv",
				  "connect_timeout", "compress",
				  "named_pipe", "init_command",
				  "read_default_file", "read_default_group",
				  "client_flag", "ssl",
				  "local_infile",
				  NULL } ;
	int connect_timeout = 0;
	int compress = -1, named_pipe = -1, local_infile = -1;
	char *init_command=NULL,
	     *read_default_file=NULL,
	     *read_default_group=NULL;
	
	self->converter = NULL;
	self->open = 0;
	check_server_init(-1);
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|ssssisOiiisssiOi:connect",
					 kwlist,
					 &host, &user, &passwd, &db,
					 &port, &unix_socket, &conv,
					 &connect_timeout,
					 &compress, &named_pipe,
					 &init_command, &read_default_file,
					 &read_default_group,
					 &client_flag, &ssl,
					 &local_infile
					 ))
		return -1;

	if (!conv) 
		conv = PyDict_New();
#if PY_VERSION_HEX > 0x02000100
	else
		Py_INCREF(conv);
#endif
	if (!conv)
		return -1;
	self->converter = conv;

#define _stringsuck(d,t,s) {t=PyMapping_GetItemString(s,#d);\
        if(t){d=PyString_AsString(t);Py_DECREF(t);}\
        PyErr_Clear();}
	
	if (ssl) {
#if HAVE_OPENSSL
		PyObject *value = NULL;
		_stringsuck(ca, value, ssl);
		_stringsuck(capath, value, ssl);
		_stringsuck(cert, value, ssl);
		_stringsuck(key, value, ssl);
		_stringsuck(cipher, value, ssl);
#else
		PyErr_SetString(_mysql_NotSupportedError,
				"client library does not have SSL support");
		return -1;
#endif
	}

	Py_BEGIN_ALLOW_THREADS ;
	conn = mysql_init(&(self->connection));
	if (connect_timeout) {
		unsigned int timeout = connect_timeout;
		mysql_options(&(self->connection), MYSQL_OPT_CONNECT_TIMEOUT, 
				(char *)&timeout);
	}
	if (compress != -1) {
		mysql_options(&(self->connection), MYSQL_OPT_COMPRESS, 0);
		client_flag |= CLIENT_COMPRESS;
	}
	if (named_pipe != -1)
		mysql_options(&(self->connection), MYSQL_OPT_NAMED_PIPE, 0);
	if (init_command != NULL)
		mysql_options(&(self->connection), MYSQL_INIT_COMMAND, init_command);
	if (read_default_file != NULL)
		mysql_options(&(self->connection), MYSQL_READ_DEFAULT_FILE, read_default_file);
	if (read_default_group != NULL)
		mysql_options(&(self->connection), MYSQL_READ_DEFAULT_GROUP, read_default_group);

	if (local_infile != -1)
		mysql_options(&(self->connection), MYSQL_OPT_LOCAL_INFILE, (char *) &local_infile);

#if HAVE_OPENSSL
	if (ssl)
		mysql_ssl_set(&(self->connection),
			      key, cert, ca, capath, cipher);
#endif

	conn = mysql_real_connect(&(self->connection), host, user, passwd, db,
				  port, unix_socket, client_flag);

	Py_END_ALLOW_THREADS ;

	if (!conn) {
		_mysql_Exception(self);
		return -1;
	}
	/*
	  PyType_GenericAlloc() automatically sets up GC allocation and
	  tracking for GC objects, at least in 2.2.1, so it does not need to
	  be done here. tp_dealloc still needs to call PyObject_GC_UnTrack(),
	  however.
	*/
	self->open = 1;
	return 0;
}

char _mysql_connect__doc__[] =
"Returns a MYSQL connection object. Exclusive use of\n\
keyword parameters strongly recommended. Consult the\n\
MySQL C API documentation for more details.\n\
\n\
host\n\
  string, host to connect\n\
\n\
user\n\
  string, user to connect as\n\
\n\
passwd\n\
  string, password to use\n\
\n\
db\n\
  string, database to use\n\
\n\
port\n\
  integer, TCP/IP port to connect to\n\
\n\
unix_socket\n\
  string, location of unix_socket (UNIX-ish only)\n\
\n\
conv\n\
  mapping, maps MySQL FIELD_TYPE.* to Python functions which\n\
  convert a string to the appropriate Python type\n\
\n\
connect_timeout\n\
  number of seconds to wait before the connection\n\
  attempt fails.\n\
\n\
compress\n\
  if set, gzip compression is enabled\n\
\n\
named_pipe\n\
  if set, connect to server via named pipe (Windows only)\n\
\n\
init_command\n\
  command which is run once the connection is created\n\
\n\
read_default_file\n\
  see the MySQL documentation for mysql_options()\n\
\n\
read_default_group\n\
  see the MySQL documentation for mysql_options()\n\
\n\
client_flag\n\
  client flags from MySQLdb.constants.CLIENT\n\
\n\
load_infile\n\
  int, non-zero enables LOAD LOCAL INFILE, zero disables\n\
\n\
";

PyObject *
_mysql_connect(
	PyObject *self,
	PyObject *args,
	PyObject *kwargs)
{
	_mysql_ConnectionObject *c=NULL;
	
	c = MyAlloc(_mysql_ConnectionObject, _mysql_ConnectionObject_Type);
	if (c == NULL) return NULL;
	if (_mysql_ConnectionObject_Initialize(c, args, kwargs)) {
		Py_DECREF(c);
		c = NULL;
	}
	return (PyObject *) c;
}

#if PY_VERSION_HEX >= 0x02020000
static int _mysql_ConnectionObject_traverse(
	_mysql_ConnectionObject *self,
	visitproc visit,
	void *arg)
{
	if (self->converter)
		return visit(self->converter, arg);
	return 0;
}
#endif

static int _mysql_ConnectionObject_clear(
	_mysql_ConnectionObject *self)
{
	Py_XDECREF(self->converter);
	self->converter = NULL;
	return 0;
}

extern PyObject *
_escape_item(
	PyObject *item,
	PyObject *d);
	
char _mysql_escape__doc__[] =
"escape(obj, dict) -- escape any special characters in object obj\n\
using mapping dict to provide quoting functions for each type.\n\
Returns a SQL literal string.";
PyObject *
_mysql_escape(
	PyObject *self,
	PyObject *args)
{
	PyObject *o=NULL, *d=NULL;
	if (!PyArg_ParseTuple(args, "O|O:escape", &o, &d))
		return NULL;
	if (d) {
		if (!PyMapping_Check(d)) {
			PyErr_SetString(PyExc_TypeError,
					"argument 2 must be a mapping");
			return NULL;
		}
		return _escape_item(o, d);
	} else {
		if (!self) {
			PyErr_SetString(PyExc_TypeError,
					"argument 2 must be a mapping");
			return NULL;
		}
		return _escape_item(o,
			   ((_mysql_ConnectionObject *) self)->converter);
	}
}

char _mysql_escape_string__doc__[] =
"escape_string(s) -- quote any SQL-interpreted characters in string s.\n\
\n\
Use connection.escape_string(s), if you use it at all.\n\
_mysql.escape_string(s) cannot handle character sets. You are\n\
probably better off using connection.escape(o) instead, since\n\
it will escape entire sequences as well as strings.";

PyObject *
_mysql_escape_string(
        _mysql_ConnectionObject *self,
        PyObject *args)
{
        PyObject *str;
        char *in, *out;
        int len, size;
        if (!PyArg_ParseTuple(args, "s#:escape_string", &in, &size)) return NULL;
        str = PyString_FromStringAndSize((char *) NULL, size*2+1);
        if (!str) return PyErr_NoMemory();
        out = PyString_AS_STRING(str);
#if MYSQL_VERSION_ID < 32321
        len = mysql_escape_string(out, in, size);
#else
        check_server_init(NULL);
        if (self && self->open)
                len = mysql_real_escape_string(&(self->connection), out, in, size);
        else
                len = mysql_escape_string(out, in, size);
#endif
        if (_PyString_Resize(&str, len) < 0) return NULL;
        return (str);
}

char _mysql_string_literal__doc__[] =
"string_literal(obj) -- converts object obj into a SQL string literal.\n\
This means, any special SQL characters are escaped, and it is enclosed\n\
within single quotes. In other words, it performs:\n\
\n\
\"'%s'\" % escape_string(str(obj))\n\
\n\
Use connection.string_literal(obj), if you use it at all.\n\
_mysql.string_literal(obj) cannot handle character sets.";

PyObject *
_mysql_string_literal(
        _mysql_ConnectionObject *self,
        PyObject *args)
{
        PyObject *str, *s, *o, *d;
        char *in, *out;
        int len, size;
        if (!PyArg_ParseTuple(args, "O|O:string_literal", &o, &d)) return NULL;
        s = PyObject_Str(o);
        if (!s) return NULL;
        in = PyString_AsString(s);
        size = PyString_GET_SIZE(s);
        str = PyString_FromStringAndSize((char *) NULL, size*2+3);
        if (!str) return PyErr_NoMemory();
        out = PyString_AS_STRING(str);
#if MYSQL_VERSION_ID < 32321
        len = mysql_escape_string(out+1, in, size);
#else
        check_server_init(NULL);
        if (self && self->open)
                len = mysql_real_escape_string(&(self->connection), out+1, in, size);
        else
                len = mysql_escape_string(out+1, in, size);
#endif
        *out = *(out+len+1) = '\'';
        if (_PyString_Resize(&str, len+2) < 0) return NULL;
        Py_DECREF(s);
        return (str);
}

static char _mysql_ConnectionObject_close__doc__[] =
"Close the connection. No further activity possible.";

static PyObject *
_mysql_ConnectionObject_close(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	if (self->open) {
		Py_BEGIN_ALLOW_THREADS
		mysql_close(&(self->connection));
		Py_END_ALLOW_THREADS
		self->open = 0;
	} else {
		PyErr_SetString(_mysql_ProgrammingError,
				"closing a closed connection");
		return NULL;
	}
	_mysql_ConnectionObject_clear(self);
	Py_INCREF(Py_None);
	return Py_None;
}

static char _mysql_ConnectionObject_affected_rows__doc__ [] =
"Return number of rows affected by the last query.\n\
Non-standard. Use Cursor.rowcount.\n\
";

static PyObject *
_mysql_ConnectionObject_affected_rows(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	check_connection(self);
	return PyLong_FromUnsignedLongLong(mysql_affected_rows(&(self->connection)));
}

static char _mysql_ConnectionObject_dump_debug_info__doc__[] =
"Instructs the server to write some debug information to the\n\
log. The connected user must have the process privilege for\n\
this to work. Non-standard.\n\
";

static PyObject *
_mysql_ConnectionObject_dump_debug_info(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	int err;

	check_connection(self);
	Py_BEGIN_ALLOW_THREADS
	err = mysql_dump_debug_info(&(self->connection));
	Py_END_ALLOW_THREADS
	if (err) return _mysql_Exception(self);
	Py_INCREF(Py_None);
	return Py_None;
}

static char _mysql_ConnectionObject_autocommit__doc__[] =
"Set the autocommit mode. True values enable; False value disable.\n\
";
static PyObject *
_mysql_ConnectionObject_autocommit(
	_mysql_ConnectionObject *self,
	PyObject *args)
{
	int flag, err;
	if (!PyArg_ParseTuple(args, "i", &flag)) return NULL;
	Py_BEGIN_ALLOW_THREADS
#if MYSQL_VERSION_ID >= 40100
	err = mysql_autocommit(&(self->connection), flag);
#else
	{
		char query[256];
		snprintf(query, 256, "SET AUTOCOMMIT=%d", flag);
		err = mysql_query(&(self->connection), query);
	}
#endif
	Py_END_ALLOW_THREADS
	if (err) return _mysql_Exception(self);
	Py_INCREF(Py_None);
	return Py_None;
}

static char _mysql_ConnectionObject_commit__doc__[] =
"Commits the current transaction\n\
";
static PyObject *
_mysql_ConnectionObject_commit(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	int err;

	Py_BEGIN_ALLOW_THREADS
#if MYSQL_VERSION_ID >= 40100
	err = mysql_commit(&(self->connection));
#else
	err = mysql_query(&(self->connection), "COMMIT");
#endif
	Py_END_ALLOW_THREADS
	if (err) return _mysql_Exception(self);
	Py_INCREF(Py_None);
	return Py_None;
}

static char _mysql_ConnectionObject_rollback__doc__[] =
"Rolls backs the current transaction\n\
";
static PyObject *
_mysql_ConnectionObject_rollback(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	int err;

	Py_BEGIN_ALLOW_THREADS
#if MYSQL_VERSION_ID >= 40100
	err = mysql_rollback(&(self->connection));
#else
	err = mysql_query(&(self->connection), "ROLLBACK");
#endif
	Py_END_ALLOW_THREADS
	if (err) return _mysql_Exception(self);
	Py_INCREF(Py_None);
	return Py_None;
}

static char _mysql_ConnectionObject_next_result__doc__[] =
"If more query results exist, next_result() reads the next query\n\
results and returns the status back to application.\n\
\n\
After calling next_result() the state of the connection is as if\n\
you had called query() for the next query. This means that you can\n\
now call store_result(), warning_count(), affected_rows()\n\
, and so forth. \n\
\n\
Returns 0 if there are more results; -1 if there are no more results\n\
\n\
Non-standard.\n\
";
static PyObject *
_mysql_ConnectionObject_next_result(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	int err;

	Py_BEGIN_ALLOW_THREADS
#if MYSQL_VERSION_ID >= 40100
	err = mysql_next_result(&(self->connection));
#else
	err = -1;
#endif
	Py_END_ALLOW_THREADS
	if (err > 0) return _mysql_Exception(self);
	return PyInt_FromLong(err);
}

#if MYSQL_VERSION_ID >= 40100

static char _mysql_ConnectionObject_set_server_option__doc__[] =
"set_server_option(option) -- Enables or disables an option\n\
for the connection.\n\
\n\
Non-standard.\n\
";
static PyObject *
_mysql_ConnectionObject_set_server_option(
	_mysql_ConnectionObject *self,
	PyObject *args)
{
	int err, flags=0;
	if (!PyArg_ParseTuple(args, "i", &flags))
		return NULL;
	Py_BEGIN_ALLOW_THREADS
	err = mysql_set_server_option(&(self->connection), flags);
	Py_END_ALLOW_THREADS
	if (err) return _mysql_Exception(self);
	return PyInt_FromLong(err);
}		

static char _mysql_ConnectionObject_sqlstate__doc__[] =
"Returns a string containing the SQLSTATE error code\n\
for the last error. The error code consists of five characters.\n\
'00000' means \"no error.\" The values are specified by ANSI SQL\n\
and ODBC. For a list of possible values, see section 23\n\
Error Handling in MySQL in the MySQL Manual.\n\
\n\
Note that not all MySQL errors are yet mapped to SQLSTATE's.\n\
The value 'HY000' (general error) is used for unmapped errors.\n\
\n\
Non-standard.\n\
";
static PyObject *
_mysql_ConnectionObject_sqlstate(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	return PyString_FromString(mysql_sqlstate(&(self->connection)));
}

static char _mysql_ConnectionObject_warning_count__doc__[] =
"Returns the number of warnings generated during execution\n\
of the previous SQL statement.\n\
\n\
Non-standard.\n\
";
static PyObject *
_mysql_ConnectionObject_warning_count(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	return PyInt_FromLong(mysql_warning_count(&(self->connection)));
}

#endif

static char _mysql_ConnectionObject_errno__doc__[] =
"Returns the error code for the most recently invoked API function\n\
that can succeed or fail. A return value of zero means that no error\n\
occurred.\n\
";

static PyObject *
_mysql_ConnectionObject_errno(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	check_connection(self);
	return PyInt_FromLong((long)mysql_errno(&(self->connection)));
}

static char _mysql_ConnectionObject_error__doc__[] =
"Returns the error message for the most recently invoked API function\n\
that can succeed or fail. An empty string ("") is returned if no error\n\
occurred.\n\
";

static PyObject *
_mysql_ConnectionObject_error(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	check_connection(self);
	return PyString_FromString(mysql_error(&(self->connection)));
}

#if MYSQL_VERSION_ID >= 32303

static char _mysql_ConnectionObject_change_user__doc__[] =
"Changes the user and causes the database specified by db to\n\
become the default (current) database on the connection\n\
specified by mysql. In subsequent queries, this database is\n\
the default for table references that do not include an\n\
explicit database specifier.\n\
\n\
This function was introduced in MySQL Version 3.23.3.\n\
\n\
Fails unless the connected user can be authenticated or if he\n\
doesn't have permission to use the database. In this case the\n\
user and database are not changed.\n\
\n\
The db parameter may be set to None if you don't want to have\n\
a default database.\n\
";

static PyObject *
_mysql_ConnectionObject_change_user(
	_mysql_ConnectionObject *self,
	PyObject *args,
	PyObject *kwargs)
{
	char *user, *pwd=NULL, *db=NULL;
	int r;
        static char *kwlist[] = { "user", "passwd", "db", NULL } ;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|ss:change_user",
					 kwlist, &user, &pwd, &db))
		return NULL;
	check_connection(self);
	Py_BEGIN_ALLOW_THREADS
		r = mysql_change_user(&(self->connection), user, pwd, db);
	Py_END_ALLOW_THREADS
	if (r) 	return _mysql_Exception(self);
	Py_INCREF(Py_None);
	return Py_None;
}
#endif

static char _mysql_ConnectionObject_character_set_name__doc__[] =
"Returns the default character set for the current connection.\n\
Non-standard.\n\
";

static PyObject *
_mysql_ConnectionObject_character_set_name(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	const char *s;

	check_connection(self);
#if MYSQL_VERSION_ID >= 32321
	s = mysql_character_set_name(&(self->connection));
#else
	s = "latin1";
#endif
	return PyString_FromString(s);
}

#if MYSQL_VERSION_ID >= 50007
static char _mysql_ConnectionObject_set_character_set__doc__[] =
"Sets the default character set for the current connection.\n\
Non-standard.\n\
";

static PyObject *
_mysql_ConnectionObject_set_character_set(
	_mysql_ConnectionObject *self,
	PyObject *args)
{
	const char *s;
	int err;
	if (!PyArg_ParseTuple(args, "s", &s)) return NULL;
	check_connection(self);
	Py_BEGIN_ALLOW_THREADS
	err = mysql_set_character_set(&(self->connection), s);
	Py_END_ALLOW_THREADS
	if (err) return _mysql_Exception(self);
	Py_INCREF(Py_None);
	return Py_None;
}
#endif

#if MYSQL_VERSION_ID >= 50010
static char _mysql_ConnectionObject_get_character_set_info__doc__[] =
"Returns a dict with information about the current character set:\n\
\n\
collation\n\
    collation name\n\
name\n\
    character set name\n\
comment\n\
    comment or descriptive name\n\
dir\n\
    character set directory\n\
mbminlen\n\
    min. length for multibyte string\n\
mbmaxlen\n\
    max. length for multibyte string\n\
\n\
Not all keys may be present, particularly dir.\n\
\n\
Non-standard.\n\
";

static PyObject *
_mysql_ConnectionObject_get_character_set_info(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	PyObject *result;
	MY_CHARSET_INFO cs;

	check_connection(self);
	mysql_get_character_set_info(&(self->connection), &cs);
	if (!(result = PyDict_New())) return NULL;
	if (cs.csname)
		PyDict_SetItemString(result, "name", PyString_FromString(cs.csname));
	if (cs.name)
		PyDict_SetItemString(result, "collation", PyString_FromString(cs.name));
	if (cs.comment)
		PyDict_SetItemString(result, "comment", PyString_FromString(cs.comment));
	if (cs.dir)
		PyDict_SetItemString(result, "dir", PyString_FromString(cs.dir));
	PyDict_SetItemString(result, "mbminlen", PyInt_FromLong(cs.mbminlen));
	PyDict_SetItemString(result, "mbmaxlen", PyInt_FromLong(cs.mbmaxlen));
	return result;
}
#endif

static char _mysql_ConnectionObject_get_host_info__doc__[] =
"Returns a string that represents the MySQL client library\n\
version. Non-standard.\n\
";

static PyObject *
_mysql_ConnectionObject_get_host_info(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	check_connection(self);
	return PyString_FromString(mysql_get_host_info(&(self->connection)));
}

static char _mysql_ConnectionObject_get_proto_info__doc__[] =
"Returns an unsigned integer representing the protocol version\n\
used by the current connection. Non-standard.\n\
";

static PyObject *
_mysql_ConnectionObject_get_proto_info(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	check_connection(self);
	return PyInt_FromLong((long)mysql_get_proto_info(&(self->connection)));
}

static char _mysql_ConnectionObject_get_server_info__doc__[] =
"Returns a string that represents the server version number.\n\
Non-standard.\n\
";

static PyObject *
_mysql_ConnectionObject_get_server_info(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	check_connection(self);
	return PyString_FromString(mysql_get_server_info(&(self->connection)));
}

static char _mysql_ConnectionObject_info__doc__[] =
"Retrieves a string providing information about the most\n\
recently executed query. Non-standard. Use messages or\n\
Cursor.messages.\n\
";

static PyObject *
_mysql_ConnectionObject_info(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	const char *s;

	check_connection(self);
	s = mysql_info(&(self->connection));
	if (s) return PyString_FromString(s);
	Py_INCREF(Py_None);
	return Py_None;
}

static char _mysql_ConnectionObject_insert_id__doc__[] =
"Returns the ID generated for an AUTO_INCREMENT column by the previous\n\
query. Use this function after you have performed an INSERT query into a\n\
table that contains an AUTO_INCREMENT field.\n\
\n\
Note that this returns 0 if the previous query does not\n\
generate an AUTO_INCREMENT value. If you need to save the value for\n\
later, be sure to call this immediately after the query\n\
that generates the value.\n\
\n\
The ID is updated after INSERT and UPDATE statements that generate\n\
an AUTO_INCREMENT value or that set a column value to\n\
LAST_INSERT_ID(expr). See section 6.3.5.2 Miscellaneous Functions\n\
in the MySQL documentation.\n\
\n\
Also note that the value of the SQL LAST_INSERT_ID() function always\n\
contains the most recently generated AUTO_INCREMENT value, and is not\n\
reset between queries because the value of that function is maintained\n\
in the server.\n\
" ;

static PyObject *
_mysql_ConnectionObject_insert_id(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	my_ulonglong r;

	check_connection(self);
	Py_BEGIN_ALLOW_THREADS
	r = mysql_insert_id(&(self->connection));
	Py_END_ALLOW_THREADS
	return PyLong_FromUnsignedLongLong(r);
}

static char _mysql_ConnectionObject_kill__doc__[] =
"Asks the server to kill the thread specified by pid.\n\
Non-standard.";

static PyObject *
_mysql_ConnectionObject_kill(
	_mysql_ConnectionObject *self,
	PyObject *args)
{
	unsigned long pid;
	int r;
	if (!PyArg_ParseTuple(args, "i:kill", &pid)) return NULL;
	check_connection(self);
	Py_BEGIN_ALLOW_THREADS
	r = mysql_kill(&(self->connection), pid);
	Py_END_ALLOW_THREADS
	if (r) return _mysql_Exception(self);
	Py_INCREF(Py_None);
	return Py_None;
}

static char _mysql_ConnectionObject_field_count__doc__[] =
"Returns the number of columns for the most recent query on the\n\
connection. Non-standard. Will probably give you bogus results\n\
on most cursor classes. Use Cursor.rowcount.\n\
";

static PyObject *
_mysql_ConnectionObject_field_count(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	check_connection(self);
#if MYSQL_VERSION_ID < 32224
	return PyInt_FromLong((long)mysql_num_fields(&(self->connection)));
#else
	return PyInt_FromLong((long)mysql_field_count(&(self->connection)));
#endif
}

static char _mysql_ConnectionObject_ping__doc__[] =
"Checks whether or not the connection to the server is\n\
working. If it has gone down, an automatic reconnection is\n\
attempted.\n\
\n\
This function can be used by clients that remain idle for a\n\
long while, to check whether or not the server has closed the\n\
connection and reconnect if necessary.\n\
\n\
New in 1.2.2: Accepts an optional reconnect parameter. If True,\n\
then the client will attempt reconnection. Note that this setting\n\
is persistent. By default, this is on in MySQL<5.0.3, and off\n\
thereafter.\n\
\n\
Non-standard. You should assume that ping() performs an\n\
implicit rollback; use only when starting a new transaction.\n\
You have been warned.\n\
";

static PyObject *
_mysql_ConnectionObject_ping(
	_mysql_ConnectionObject *self,
	PyObject *args)
{
	int r, reconnect = -1;
	if (!PyArg_ParseTuple(args, "|I", &reconnect)) return NULL;
	check_connection(self);
	if ( reconnect != -1 ) self->connection.reconnect = reconnect;
	Py_BEGIN_ALLOW_THREADS
	r = mysql_ping(&(self->connection));
	Py_END_ALLOW_THREADS
	if (r) 	return _mysql_Exception(self);
	Py_INCREF(Py_None);
	return Py_None;
}

static char _mysql_ConnectionObject_query__doc__[] =
"Execute a query. store_result() or use_result() will get the\n\
result set, if any. Non-standard. Use cursor() to create a cursor,\n\
then cursor.execute().\n\
" ;

static PyObject *
_mysql_ConnectionObject_query(
	_mysql_ConnectionObject *self,
	PyObject *args)
{
	char *query;
	int len, r;
	if (!PyArg_ParseTuple(args, "s#:query", &query, &len)) return NULL;
	check_connection(self);
	Py_BEGIN_ALLOW_THREADS
	r = mysql_real_query(&(self->connection), query, len);
	Py_END_ALLOW_THREADS
	if (r) return _mysql_Exception(self);
	Py_INCREF(Py_None);
	return Py_None;
}


static char _mysql_ConnectionObject_select_db__doc__[] =
"Causes the database specified by db to become the default\n\
(current) database on the connection specified by mysql. In subsequent\n\
queries, this database is the default for table references that do not\n\
include an explicit database specifier.\n\
\n\
Fails unless the connected user can be authenticated as having\n\
permission to use the database.\n\
\n\
Non-standard.\n\
";

static PyObject *
_mysql_ConnectionObject_select_db(
	_mysql_ConnectionObject *self,
	PyObject *args)
{
	char *db;
	int r;
	if (!PyArg_ParseTuple(args, "s:select_db", &db)) return NULL;
	check_connection(self);
	Py_BEGIN_ALLOW_THREADS
	r = mysql_select_db(&(self->connection), db);
	Py_END_ALLOW_THREADS
	if (r) 	return _mysql_Exception(self);
	Py_INCREF(Py_None);
	return Py_None;
}

static char _mysql_ConnectionObject_shutdown__doc__[] =
"Asks the database server to shut down. The connected user must\n\
have shutdown privileges. Non-standard.\n\
";

static PyObject *
_mysql_ConnectionObject_shutdown(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	int r;

	check_connection(self);
	Py_BEGIN_ALLOW_THREADS
	r = mysql_shutdown(&(self->connection)
#if MYSQL_VERSION_ID >= 40103
		, SHUTDOWN_DEFAULT
#endif
		);
	Py_END_ALLOW_THREADS
	if (r) return _mysql_Exception(self);
	Py_INCREF(Py_None);
	return Py_None;
}

static char _mysql_ConnectionObject_stat__doc__[] =
"Returns a character string containing information similar to\n\
that provided by the mysqladmin status command. This includes\n\
uptime in seconds and the number of running threads,\n\
questions, reloads, and open tables. Non-standard.\n\
";

static PyObject *
_mysql_ConnectionObject_stat(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	const char *s;

	check_connection(self);
	Py_BEGIN_ALLOW_THREADS
	s = mysql_stat(&(self->connection));
	Py_END_ALLOW_THREADS
	if (!s) return _mysql_Exception(self);
	return PyString_FromString(s);
}

static char _mysql_ConnectionObject_store_result__doc__[] =
"Returns a result object acquired by mysql_store_result\n\
(results stored in the client). If no results are available,\n\
None is returned. Non-standard.\n\
";

static PyObject *
_mysql_ConnectionObject_store_result(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	PyObject *arglist=NULL, *kwarglist=NULL, *result=NULL;
	_mysql_ResultObject *r=NULL;

	check_connection(self);
	arglist = Py_BuildValue("(OiO)", self, 0, self->converter);
	if (!arglist) goto error;
	kwarglist = PyDict_New();
	if (!kwarglist) goto error;
	r = MyAlloc(_mysql_ResultObject, _mysql_ResultObject_Type);
	if (!r) goto error;
	if (_mysql_ResultObject_Initialize(r, arglist, kwarglist))
		goto error;
	result = (PyObject *) r;
	if (!(r->result)) {
		Py_DECREF(result);
		Py_INCREF(Py_None);
		result = Py_None;
	}
  error:
	Py_XDECREF(arglist);
	Py_XDECREF(kwarglist);
	return result;
}

static char _mysql_ConnectionObject_thread_id__doc__[] =
"Returns the thread ID of the current connection. This value\n\
can be used as an argument to kill() to kill the thread.\n\
\n\
If the connection is lost and you reconnect with ping(), the\n\
thread ID will change. This means you should not get the\n\
thread ID and store it for later. You should get it when you\n\
need it.\n\
\n\
Non-standard.";

static PyObject *
_mysql_ConnectionObject_thread_id(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	unsigned long pid;

	check_connection(self);
	Py_BEGIN_ALLOW_THREADS
	pid = mysql_thread_id(&(self->connection));
	Py_END_ALLOW_THREADS
	return PyInt_FromLong((long)pid);
}

static char _mysql_ConnectionObject_use_result__doc__[] =
"Returns a result object acquired by mysql_use_result\n\
(results stored in the server). If no results are available,\n\
None is returned. Non-standard.\n\
";

static PyObject *
_mysql_ConnectionObject_use_result(
	_mysql_ConnectionObject *self,
	PyObject *unused)
{
	PyObject *arglist=NULL, *kwarglist=NULL, *result=NULL;
	_mysql_ResultObject *r=NULL;

	check_connection(self);
	arglist = Py_BuildValue("(OiO)", self, 1, self->converter);
	if (!arglist) return NULL;
	kwarglist = PyDict_New();
	if (!kwarglist) goto error;
	r = MyAlloc(_mysql_ResultObject, _mysql_ResultObject_Type);
	if (!r) goto error;
	result = (PyObject *) r;
	if (_mysql_ResultObject_Initialize(r, arglist, kwarglist))
		goto error;
	if (!(r->result)) {
		Py_DECREF(result);
		Py_INCREF(Py_None);
		result = Py_None;
	}
  error:
	Py_DECREF(arglist);
	Py_XDECREF(kwarglist);
	return result;
}

static void
_mysql_ConnectionObject_dealloc(
	_mysql_ConnectionObject *self)
{
	PyObject *o;

	PyObject_GC_UnTrack(self);
	if (self->open) {
		o = _mysql_ConnectionObject_close(self, NULL);
		Py_XDECREF(o);
	}
	MyFree(self);
}

static PyObject *
_mysql_ConnectionObject_repr(
	_mysql_ConnectionObject *self)
{
	char buf[300];
	if (self->open)
		sprintf(buf, "<_mysql.connection open to '%.256s' at %lx>",
			self->connection.host,
			(long)self);
	else
		sprintf(buf, "<_mysql.connection closed at %lx>",
			(long)self);
	return PyString_FromString(buf);
}

static PyMethodDef _mysql_ConnectionObject_methods[] = {
	{
		"affected_rows",
		(PyCFunction)_mysql_ConnectionObject_affected_rows,
		METH_NOARGS,
		_mysql_ConnectionObject_affected_rows__doc__
	},
	{
		"autocommit",
		(PyCFunction)_mysql_ConnectionObject_autocommit,
		METH_VARARGS,
		_mysql_ConnectionObject_autocommit__doc__
	},
	{
		"commit",
		(PyCFunction)_mysql_ConnectionObject_commit,
		METH_NOARGS,
		_mysql_ConnectionObject_commit__doc__
	},
	{
		"rollback",
		(PyCFunction)_mysql_ConnectionObject_rollback,
		METH_NOARGS,
		_mysql_ConnectionObject_rollback__doc__
	},
	{
		"next_result",
		(PyCFunction)_mysql_ConnectionObject_next_result,
		METH_NOARGS,
		_mysql_ConnectionObject_next_result__doc__
	},
#if MYSQL_VERSION_ID >= 40100
	{
		"set_server_option",
		(PyCFunction)_mysql_ConnectionObject_set_server_option,
		METH_VARARGS,
		_mysql_ConnectionObject_set_server_option__doc__
	},
	{
		"sqlstate",
		(PyCFunction)_mysql_ConnectionObject_sqlstate,
		METH_NOARGS,
		_mysql_ConnectionObject_sqlstate__doc__
	},
	{
		"warning_count",
		(PyCFunction)_mysql_ConnectionObject_warning_count,
		METH_NOARGS,
		_mysql_ConnectionObject_warning_count__doc__
	},
#endif
#if MYSQL_VERSION_ID >= 32303
	{
		"change_user",
		(PyCFunction)_mysql_ConnectionObject_change_user,
		METH_VARARGS | METH_KEYWORDS,
		_mysql_ConnectionObject_change_user__doc__
	},
#endif
	{
		"character_set_name",
		(PyCFunction)_mysql_ConnectionObject_character_set_name,
		METH_NOARGS,
		_mysql_ConnectionObject_character_set_name__doc__
	},
#if MYSQL_VERSION_ID >= 50007
	{
		"set_character_set",
		(PyCFunction)_mysql_ConnectionObject_set_character_set,
		METH_VARARGS,
		_mysql_ConnectionObject_set_character_set__doc__
	},
#endif
#if MYSQL_VERSION_ID >= 50010
	{
		"get_character_set_info",
		(PyCFunction)_mysql_ConnectionObject_get_character_set_info,
		METH_VARARGS,
		_mysql_ConnectionObject_get_character_set_info__doc__
	},
#endif
	{
		"close",
		(PyCFunction)_mysql_ConnectionObject_close,
		METH_NOARGS,
		_mysql_ConnectionObject_close__doc__
	},
	{
		"dump_debug_info",
		(PyCFunction)_mysql_ConnectionObject_dump_debug_info,
		METH_NOARGS,
		_mysql_ConnectionObject_dump_debug_info__doc__
	},
	{
		"escape",
		(PyCFunction)_mysql_escape,
		METH_VARARGS,
		_mysql_escape__doc__
	},
	{
		"escape_string",
		(PyCFunction)_mysql_escape_string,
		METH_VARARGS,
		_mysql_escape_string__doc__
	},
	{
		"error",
		(PyCFunction)_mysql_ConnectionObject_error,
		METH_NOARGS,
		_mysql_ConnectionObject_error__doc__
	},
	{
		"errno",
		(PyCFunction)_mysql_ConnectionObject_errno,
		METH_NOARGS,
		_mysql_ConnectionObject_errno__doc__
	},
	{
		"field_count",
		(PyCFunction)_mysql_ConnectionObject_field_count,
		METH_NOARGS,
		_mysql_ConnectionObject_field_count__doc__
	},
	{
		"get_host_info",
		(PyCFunction)_mysql_ConnectionObject_get_host_info,
		METH_NOARGS,
		_mysql_ConnectionObject_get_host_info__doc__
	},
	{
		"get_proto_info",
		(PyCFunction)_mysql_ConnectionObject_get_proto_info,
		METH_NOARGS,
		_mysql_ConnectionObject_get_proto_info__doc__
	},
	{
		"get_server_info",
		(PyCFunction)_mysql_ConnectionObject_get_server_info,
		METH_NOARGS,
		_mysql_ConnectionObject_get_server_info__doc__
	},
	{
		"info",
		(PyCFunction)_mysql_ConnectionObject_info,
		METH_NOARGS,
		_mysql_ConnectionObject_info__doc__
	},
	{
		"insert_id",
		(PyCFunction)_mysql_ConnectionObject_insert_id,
		METH_NOARGS,
		_mysql_ConnectionObject_insert_id__doc__
	},
	{
		"kill",
		(PyCFunction)_mysql_ConnectionObject_kill,
		METH_VARARGS,
		_mysql_ConnectionObject_kill__doc__
	},
	{
		"ping",
		(PyCFunction)_mysql_ConnectionObject_ping,
		METH_VARARGS,
		_mysql_ConnectionObject_ping__doc__
	},
	{
		"query",
		(PyCFunction)_mysql_ConnectionObject_query,
		METH_VARARGS,
		_mysql_ConnectionObject_query__doc__
	},
	{
		"select_db",
		(PyCFunction)_mysql_ConnectionObject_select_db,
		METH_VARARGS,
		_mysql_ConnectionObject_select_db__doc__
	},
	{
		"shutdown",
		(PyCFunction)_mysql_ConnectionObject_shutdown,
		METH_NOARGS,
		_mysql_ConnectionObject_shutdown__doc__
	},
	{
		"stat",
		(PyCFunction)_mysql_ConnectionObject_stat,
		METH_NOARGS,
		_mysql_ConnectionObject_stat__doc__
	},
	{
		"store_result",
		(PyCFunction)_mysql_ConnectionObject_store_result,
		METH_NOARGS,
		_mysql_ConnectionObject_store_result__doc__
	},
	{
		"string_literal",
		(PyCFunction)_mysql_string_literal,
		METH_VARARGS,
		_mysql_string_literal__doc__},
	{
		"thread_id",
		(PyCFunction)_mysql_ConnectionObject_thread_id,
		METH_NOARGS,
		_mysql_ConnectionObject_thread_id__doc__
	},
	{
		"use_result",
		(PyCFunction)_mysql_ConnectionObject_use_result,
		METH_NOARGS,
		_mysql_ConnectionObject_use_result__doc__
	},
	{NULL,              NULL} /* sentinel */
};

static MyMemberlist(_mysql_ConnectionObject_memberlist)[] = {
	MyMember(
		"open",
		T_INT,
		offsetof(_mysql_ConnectionObject,open),
		RO,
		"True if connection is open"
		),
	MyMember(
		"converter",
		T_OBJECT,
		offsetof(_mysql_ConnectionObject,converter),
		0,
		"Type conversion mapping"
		),
	MyMember(
		"server_capabilities",
		T_UINT,
		offsetof(_mysql_ConnectionObject,connection.server_capabilities),
		RO,
		"Capabilites of server; consult MySQLdb.constants.CLIENT"
		),
	MyMember(
		 "port",
		 T_UINT,
		 offsetof(_mysql_ConnectionObject,connection.port),
		 RO,
		 "TCP/IP port of the server connection"
		 ),
	MyMember(
		 "client_flag",
		 T_UINT,
		 RO,
		 offsetof(_mysql_ConnectionObject,connection.client_flag),
		 "Client flags; refer to MySQLdb.constants.CLIENT"
		 ),
	{NULL} /* Sentinel */
};

static PyObject *
_mysql_ConnectionObject_getattr(
	_mysql_ConnectionObject *self,
	char *name)
{
	PyObject *res;

	res = Py_FindMethod(_mysql_ConnectionObject_methods, (PyObject *)self, name);
	if (res != NULL)
		return res;
	PyErr_Clear();
	if (strcmp(name, "closed") == 0)
		return PyInt_FromLong((long)!(self->open));
#if PY_VERSION_HEX < 0x02020000
	return PyMember_Get((char *)self, _mysql_ConnectionObject_memberlist, name);
#else
	{
		MyMemberlist(*l);
		for (l = _mysql_ConnectionObject_memberlist; l->name != NULL; l++) {
			if (strcmp(l->name, name) == 0)
				return PyMember_GetOne((char *)self, l);
		}
		PyErr_SetString(PyExc_AttributeError, name);
		return NULL;
	}
#endif
}

static int
_mysql_ConnectionObject_setattr(
	_mysql_ConnectionObject *self,
	char *name,
	PyObject *v)
{
	if (v == NULL) {
		PyErr_SetString(PyExc_AttributeError,
				"can't delete connection attributes");
		return -1;
	}
#if PY_VERSION_HEX < 0x02020000
	return PyMember_Set((char *)self, _mysql_ConnectionObject_memberlist, name, v);
#else
        {
		MyMemberlist(*l);
		for (l = _mysql_ConnectionObject_memberlist; l->name != NULL; l++)
			if (strcmp(l->name, name) == 0)
				return PyMember_SetOne((char *)self, l, v);
	}
        PyErr_SetString(PyExc_AttributeError, name);
        return -1;
#endif
}

PyTypeObject _mysql_ConnectionObject_Type = {
	PyObject_HEAD_INIT(NULL)
	0,
	"_mysql.connection", /* (char *)tp_name For printing */
	sizeof(_mysql_ConnectionObject),
	0,
	(destructor)_mysql_ConnectionObject_dealloc, /* tp_dealloc */
	0, /*tp_print*/
	(getattrfunc)_mysql_ConnectionObject_getattr, /* tp_getattr */
	(setattrfunc)_mysql_ConnectionObject_setattr, /* tp_setattr */
	0, /*tp_compare*/
	(reprfunc)_mysql_ConnectionObject_repr, /* tp_repr */
	
	/* Method suites for standard classes */
	
	0, /* (PyNumberMethods *) tp_as_number */
	0, /* (PySequenceMethods *) tp_as_sequence */
	0, /* (PyMappingMethods *) tp_as_mapping */
	
	/* More standard operations (here for binary compatibility) */
	
	0, /* (hashfunc) tp_hash */
	0, /* (ternaryfunc) tp_call */
	0, /* (reprfunc) tp_str */
	0, /* (getattrofunc) tp_getattro */
	0, /* (setattrofunc) tp_setattro */
	
	/* Functions to access object as input/output buffer */
	0, /* (PyBufferProcs *) tp_as_buffer */
	
	/* Flags to define presence of optional/expanded features */
#if PY_VERSION_HEX < 0x02020000
	Py_TPFLAGS_DEFAULT, /* (long) tp_flags */
#else
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_BASETYPE,
#endif
	_mysql_connect__doc__, /* (char *) tp_doc Documentation string */
#if PY_VERSION_HEX >= 0x02000000	
	/* Assigned meaning in release 2.0 */
#if PY_VERSION_HEX >= 0x02020000
	/* call function for all accessible objects */
	(traverseproc) _mysql_ConnectionObject_traverse, /* tp_traverse */
	
	/* delete references to contained objects */
	(inquiry) _mysql_ConnectionObject_clear, /* tp_clear */
#else
	/* not supporting pre-2.2 GC */
	0,
	0,
#endif
#if PY_VERSION_HEX >= 0x02010000	
	/* Assigned meaning in release 2.1 */
	/* rich comparisons */
	0, /* (richcmpfunc) tp_richcompare */
	
	/* weak reference enabler */
	0, /* (long) tp_weaklistoffset */
#if PY_VERSION_HEX >= 0x02020000
	/* Added in release 2.2 */
	/* Iterators */
	0, /* (getiterfunc) tp_iter */
	0, /* (iternextfunc) tp_iternext */
	
	/* Attribute descriptor and subclassing stuff */
	(struct PyMethodDef *)_mysql_ConnectionObject_methods, /* tp_methods */
	(MyMemberlist(*))_mysql_ConnectionObject_memberlist, /* tp_members */
	0, /* (struct getsetlist *) tp_getset; */
	0, /* (struct _typeobject *) tp_base; */
	0, /* (PyObject *) tp_dict */
	0, /* (descrgetfunc) tp_descr_get */
	0, /* (descrsetfunc) tp_descr_set */
	0, /* (long) tp_dictoffset */
	(initproc)_mysql_ConnectionObject_Initialize, /* tp_init */
	NULL, /* tp_alloc */
	NULL, /* tp_new */
	NULL, /* tp_free Low-level free-memory routine */ 
	0, /* (PyObject *) tp_bases */
	0, /* (PyObject *) tp_mro method resolution order */
	0, /* (PyObject *) tp_defined */
#endif /* python 2.2 */
#endif /* python 2.1 */
#endif /* python 2.0 */
} ;

