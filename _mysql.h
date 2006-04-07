#include "pymemcompat.h"

#ifdef MS_WIN32
#include <windows.h>
#endif /* MS_WIN32 */
#ifndef uint
#define uint unsigned int
#endif

#include "structmember.h"
#include "mysql.h"
#include "my_config.h"
#include "mysqld_error.h"
#include "errmsg.h"

#if PY_VERSION_HEX < 0x02020000
# define MyTuple_Resize(t,n,d) _PyTuple_Resize(t, n, d)
# define MyMember(a,b,c,d,e) {a,b,c,d}
# define MyMemberlist(x) struct memberlist x
# define MyAlloc(s,t) PyObject_New(s,&t)
# define MyFree(o) PyObject_Del(o)
#else
# define MyTuple_Resize(t,n,d) _PyTuple_Resize(t, n)
# define MyMember(a,b,c,d,e) {a,b,c,d,e}
# define MyMemberlist(x) struct PyMemberDef x
# define MyAlloc(s,t) (s *) t.tp_alloc(&t,0)
# define MyFree(ob) ob->ob_type->tp_free((PyObject *)ob) 
#endif

typedef struct {
	PyObject_HEAD
	MYSQL connection;
	int open;
	PyObject *converter;
} _mysql_ConnectionObject;

#define check_connection(c) if (!(c->open)) return _mysql_Exception(c)
#define result_connection(r) ((_mysql_ConnectionObject *)r->conn)
#define check_result_connection(r) check_connection(result_connection(r))

extern PyTypeObject _mysql_ConnectionObject_Type;

typedef struct {
	PyObject_HEAD
	PyObject *conn;
	MYSQL_RES *result;
	int nfields;
	int use;
	PyObject *converter;
} _mysql_ResultObject;

extern PyTypeObject _mysql_ResultObject_Type;

int _mysql_server_init_done;
#if MYSQL_VERSION_ID >= 40000
#define check_server_init(x) if (!_mysql_server_init_done) { if (mysql_server_init(0, NULL, NULL)) { _mysql_Exception(NULL); return x; } else { _mysql_server_init_done = 1;} }
#else
#define check_server_init(x) if (!_mysql_server_init_done) _mysql_server_init_done = 1
#endif

PyObject *_mysql_MySQLError;
 PyObject *_mysql_Warning;
 PyObject *_mysql_Error;
 PyObject *_mysql_DatabaseError;
 PyObject *_mysql_InterfaceError; 
 PyObject *_mysql_DataError;
 PyObject *_mysql_OperationalError; 
 PyObject *_mysql_IntegrityError; 
 PyObject *_mysql_InternalError; 
 PyObject *_mysql_ProgrammingError;
 PyObject *_mysql_NotSupportedError;

extern PyObject *
_mysql_Exception(_mysql_ConnectionObject *c);
