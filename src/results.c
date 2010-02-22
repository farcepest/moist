/* -*- mode: C; indent-tabs-mode: t; c-basic-offset: 8; -*- */

#include "mysqlmod.h"

static PyObject *
_mysql_ResultObject_get_fields(
	_mysql_ResultObject *self,
	PyObject *unused)
{
	PyObject *arglist=NULL, *kwarglist=NULL;
	PyObject *fields=NULL;
	_mysql_FieldObject *field=NULL;
	unsigned int i, n;

	check_result_connection(self);
	kwarglist = PyDict_New();
	if (!kwarglist) goto error;
	n = mysql_num_fields(self->result);
	if (!(fields = PyTuple_New(n))) return NULL;
	for (i=0; i<n; i++) {
		arglist = Py_BuildValue("(Oi)", self, i);
		if (!arglist) goto error;
		field = MyAlloc(_mysql_FieldObject, _mysql_FieldObject_Type);
		if (!field) goto error;
		if (_mysql_FieldObject_Initialize(field, arglist, kwarglist))
			goto error;
		Py_DECREF(arglist);
		PyTuple_SET_ITEM(fields, i, (PyObject *) field);
	}
	Py_DECREF(kwarglist);
	return fields;
  error:
	Py_XDECREF(arglist);
	Py_XDECREF(kwarglist);
	Py_XDECREF(fields);
	return NULL;
}

static char _mysql_ResultObject__doc__[] =
"result(connection, use=0) -- Result set from a query.\n\
\n\
Creating instances of this class directly is an excellent way to\n\
shoot yourself in the foot. If using _mysql.connection directly,\n\
use connection.store_result() or connection.use_result() instead.\n\
If using MySQLdb.Connection, this is done by the cursor class.\n\
Just forget you ever saw this. Forget... FOR-GET...";

int
_mysql_ResultObject_Initialize(
	_mysql_ResultObject *self,
	PyObject *args,
	PyObject *kwargs)
{
	static char *kwlist[] = {"connection", "use", NULL};
	MYSQL_RES *result;
	_mysql_ConnectionObject *conn = NULL;
	int use = 0;
	int n;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|i", kwlist,
					  &conn, &use))
		return -1;

	self->conn = (PyObject *) conn;
	Py_INCREF(conn);
	self->use = use;
	Py_BEGIN_ALLOW_THREADS ;
	if (use)
		result = mysql_use_result(&(conn->connection));
	else
		result = mysql_store_result(&(conn->connection));
	self->result = result;
	Py_END_ALLOW_THREADS ;
	if (!result) {
		return 0;
	}
	n = mysql_num_fields(result);
	self->nfields = n;
	self->fields = _mysql_ResultObject_get_fields(self, NULL);

	return 0;
}

static int
_mysql_ResultObject_traverse(
	_mysql_ResultObject *self,
	visitproc visit,
	void *arg)
{
	int r;
	if (self->fields) {
		if (!(r = visit(self->fields, arg))) return r;
	}
	if (self->conn)
		return visit(self->conn, arg);
	return 0;
}

static char _mysql_ResultObject_describe__doc__[] =
"Returns the sequence of 7-tuples required by the DB-API for\n\
the Cursor.description attribute.\n\
";

static PyObject *
_mysql_ResultObject_describe(
	_mysql_ResultObject *self,
	PyObject *unused)
{
	PyObject *d;
	MYSQL_FIELD *fields;
	unsigned int i, n;

	check_result_connection(self);
	n = mysql_num_fields(self->result);
	fields = mysql_fetch_fields(self->result);
	if (!(d = PyTuple_New(n))) return NULL;
	for (i=0; i<n; i++) {
		PyObject *t;
		t = Py_BuildValue("(siiiiii)",
				  fields[i].name,
				  (long) fields[i].type,
				  (long) fields[i].max_length,
				  (long) fields[i].length,
				  (long) fields[i].length,
				  (long) fields[i].decimals,
				  (long) !(IS_NOT_NULL(fields[i].flags)));
		if (!t) goto error;
		PyTuple_SET_ITEM(d, i, t);
	}
	return d;
  error:
	Py_XDECREF(d);
	return NULL;
}

static char _mysql_ResultObject_fetch_row__doc__[] =
"fetchrow()\n\
  Fetches one row as a tuple of strings.\n\
  NULL is returned as None.\n\
  A single None indicates the end of the result set.\n\
";

static PyObject *
_mysql_ResultObject_fetch_row(
	_mysql_ResultObject *self,
 	PyObject *unused)
 {
	unsigned int n, i;
	unsigned long *length;
	PyObject *r=NULL;
	MYSQL_ROW row;
	
 	check_result_connection(self);
 	
	if (!self->use)
		row = mysql_fetch_row(self->result);
	else {
 		Py_BEGIN_ALLOW_THREADS;
		row = mysql_fetch_row(self->result);
 		Py_END_ALLOW_THREADS;
	}
	if (!row && mysql_errno(&(((_mysql_ConnectionObject *)(self->conn))->connection))) {
		_mysql_Exception((_mysql_ConnectionObject *)self->conn);
		goto error;
	}
	if (!row) {
		Py_INCREF(Py_None);
		return Py_None;
	}
	
	n = mysql_num_fields(self->result);
	if (!(r = PyTuple_New(n))) return NULL;
	length = mysql_fetch_lengths(self->result);
	for (i=0; i<n; i++) {
		PyObject *v;
		if (row[i]) {
			v = PyString_FromStringAndSize(row[i], length[i]);
			if (!v) goto error;
		} else /* NULL */ {
			v = Py_None;
			Py_INCREF(v);
 		}
		PyTuple_SET_ITEM(r, i, v);
 	}
	return r;
  error:
	Py_XDECREF(r);
	return NULL;
}

static char _mysql_ResultObject_field_flags__doc__[] =
"Returns a tuple of field flags, one for each column in the result.\n\
" ;

static PyObject *
_mysql_ResultObject_field_flags(
	_mysql_ResultObject *self,
	PyObject *unused)
{
	PyObject *d;
	MYSQL_FIELD *fields;
	unsigned int i, n;

	check_result_connection(self);
	n = mysql_num_fields(self->result);
	fields = mysql_fetch_fields(self->result);
	if (!(d = PyTuple_New(n))) return NULL;
	for (i=0; i<n; i++) {
		PyObject *f;
		if (!(f = PyInt_FromLong((long)fields[i].flags))) goto error;
		PyTuple_SET_ITEM(d, i, f);
	}
	return d;
  error:
	Py_XDECREF(d);
	return NULL;
}

typedef PyObject *_PYFUNC(_mysql_ResultObject *, MYSQL_ROW);

static char _mysql_ResultObject_clear__doc__[] =
"clear()\n\
  Reads to the end of the result set, discarding all the rows.\n\
";

static PyObject *
_mysql_ResultObject_clear(
	_mysql_ResultObject *self,
	PyObject *unused)
{
	if (self->result) {
		if (self->use) {
			Py_BEGIN_ALLOW_THREADS;
			while (mysql_fetch_row(self->result));
			Py_END_ALLOW_THREADS;

			if (mysql_errno(&(((_mysql_ConnectionObject *)(self->conn))->connection))) {
				_mysql_Exception((_mysql_ConnectionObject *)self->conn);
				return NULL;
			}
		}
	}
	Py_XDECREF(self->fields);
	self->fields = NULL;
	Py_XDECREF(self->conn);
	self->conn = NULL;
	if (self->result) {
		mysql_free_result(self->result);
		self->result = NULL;
	}
	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject *
_mysql_ResultObject__iter__(
	_mysql_ResultObject *self,
	PyObject *unused)
{
	check_result_connection(self);
	Py_INCREF(self);
	return (PyObject *)self;
}

static PyObject *
_mysql_ResultObject_next(
	_mysql_ResultObject *self,
	PyObject *unused)
{
	PyObject *row;
	check_result_connection(self);
	row = _mysql_ResultObject_fetch_row(self, NULL);
	if (row == Py_None) {
		Py_DECREF(row);
		PyErr_SetString(PyExc_StopIteration, "");
		return NULL;
	}
	return row;
}

static char _mysql_ResultObject_num_fields__doc__[] =
"Returns the number of fields (column) in the result." ;

static PyObject *
_mysql_ResultObject_num_fields(
	_mysql_ResultObject *self,
	PyObject *unused)
{
	check_result_connection(self);
	return PyInt_FromLong((long)mysql_num_fields(self->result));
}

static char _mysql_ResultObject_num_rows__doc__[] =
"Returns the number of rows in the result set. Note that if\n\
use=1, this will not return a valid value until the entire result\n\
set has been read.\n\
";

static PyObject *
_mysql_ResultObject_num_rows(
	_mysql_ResultObject *self,
	PyObject *unused)
{
	check_result_connection(self);
	return PyLong_FromUnsignedLongLong(mysql_num_rows(self->result));
}


static char _mysql_ResultObject_data_seek__doc__[] =
"data_seek(n) -- seek to row n of result set";
static PyObject *
_mysql_ResultObject_data_seek(
     _mysql_ResultObject *self,
     PyObject *args)
{
	unsigned int row;
	if (!PyArg_ParseTuple(args, "i:data_seek", &row)) return NULL;
	check_result_connection(self);
	mysql_data_seek(self->result, row);
	Py_INCREF(Py_None);
	return Py_None;
}

static char _mysql_ResultObject_row_seek__doc__[] =
"row_seek(n) -- seek by offset n rows of result set";
static PyObject *
_mysql_ResultObject_row_seek(
     _mysql_ResultObject *self,
     PyObject *args)
{
	int offset;
        MYSQL_ROW_OFFSET r;
	if (!PyArg_ParseTuple(args, "i:row_seek", &offset)) return NULL;
	check_result_connection(self);
	if (self->use) {
		PyErr_SetString(_mysql_ProgrammingError,
				"cannot be used with connection.use_result()");
		return NULL;
	}
	r = mysql_row_tell(self->result);
	mysql_row_seek(self->result, r+offset);
	Py_INCREF(Py_None);
	return Py_None;
}

static char _mysql_ResultObject_row_tell__doc__[] =
"row_tell() -- return the current row number of the result set.";
static PyObject *
_mysql_ResultObject_row_tell(
	_mysql_ResultObject *self,
	PyObject *unused)
{
	MYSQL_ROW_OFFSET r;

	check_result_connection(self);
	if (self->use) {
		PyErr_SetString(_mysql_ProgrammingError,
				"cannot be used with connection.use_result()");
		return NULL;
	}
	r = mysql_row_tell(self->result);
	return PyInt_FromLong(r-self->result->data->data);
}

static void
_mysql_ResultObject_dealloc(
	_mysql_ResultObject *self)
{
	PyObject_GC_UnTrack((PyObject *)self);
	_mysql_ResultObject_clear(self, NULL);
	mysql_free_result(self->result);
	MyFree(self);
}

static PyObject *
_mysql_ResultObject_repr(
	_mysql_ResultObject *self)
{
	char buf[300];
	sprintf(buf, "<_mysql.result object at %lx>",
		(long)self);
	return PyString_FromString(buf);
}

static PyMethodDef _mysql_ResultObject_methods[] = {
	{
		"data_seek",
		(PyCFunction)_mysql_ResultObject_data_seek,
		METH_VARARGS,
		_mysql_ResultObject_data_seek__doc__
	},
	{
		"row_seek",
		(PyCFunction)_mysql_ResultObject_row_seek,
		METH_VARARGS,
		_mysql_ResultObject_row_seek__doc__
	},
	{
		"row_tell",
		(PyCFunction)_mysql_ResultObject_row_tell,
		METH_NOARGS,
		_mysql_ResultObject_row_tell__doc__
	},
	{
		"clear",
		(PyCFunction)_mysql_ResultObject_clear,
		METH_NOARGS,
		_mysql_ResultObject_clear__doc__
	},
	{
		"describe",
		(PyCFunction)_mysql_ResultObject_describe,
		METH_NOARGS,
		_mysql_ResultObject_describe__doc__
	},
	{
		"fetch_row",
		(PyCFunction)_mysql_ResultObject_fetch_row,
		METH_VARARGS | METH_KEYWORDS,
		_mysql_ResultObject_fetch_row__doc__
	},

	{
		"field_flags",
		(PyCFunction)_mysql_ResultObject_field_flags,
		METH_NOARGS,
		_mysql_ResultObject_field_flags__doc__
	},
	{
		"num_fields",
		(PyCFunction)_mysql_ResultObject_num_fields,
		METH_NOARGS,
		_mysql_ResultObject_num_fields__doc__
	},
	{
		"num_rows",
		(PyCFunction)_mysql_ResultObject_num_rows,
		METH_NOARGS,
		_mysql_ResultObject_num_rows__doc__
	},
	{NULL,              NULL} /* sentinel */
};

static struct PyMemberDef _mysql_ResultObject_memberlist[] = {
	{
		"connection",
		T_OBJECT,
		offsetof(_mysql_ConnectionObject, connection),
		RO,
		"Connection associated with result"
	},
	{
		"fields",
		T_OBJECT,
		offsetof(_mysql_ResultObject, fields),
		RO,
		"Field metadata for result set"
	},
	{
		"use",
		T_INT,
		offsetof(_mysql_ResultObject, use),
		RO,
		"True if mysql_use_result() was used; False if mysql_store_result() was used"
	},
	{NULL} /* Sentinel */
};

static PyObject *
_mysql_ResultObject_getattr(
	_mysql_ResultObject *self,
	char *name)
{
	PyObject *res;
	struct PyMemberDef *l;

	res = Py_FindMethod(_mysql_ResultObject_methods, (PyObject *)self, name);
	if (res != NULL)
		return res;
	PyErr_Clear();

	for (l = _mysql_ResultObject_memberlist; l->name != NULL; l++) {
		if (strcmp(l->name, name) == 0)
			return PyMember_GetOne((char *)self, l);
	}

	PyErr_SetString(PyExc_AttributeError, name);
	return NULL;
}

static int
_mysql_ResultObject_setattr(
	_mysql_ResultObject *self,
	char *name,
	PyObject *v)
{
	struct PyMemberDef *l;

	if (v == NULL) {
		PyErr_SetString(PyExc_AttributeError,
				"can't delete connection attributes");
		return -1;
	}

	for (l = _mysql_ResultObject_memberlist; l->name != NULL; l++)
		if (strcmp(l->name, name) == 0)
			return PyMember_SetOne((char *)self, l, v);

        PyErr_SetString(PyExc_AttributeError, name);
        return -1;
}

PyTypeObject _mysql_ResultObject_Type = {
	PyObject_HEAD_INIT(NULL)
	0,
	"_mysql.result",
	sizeof(_mysql_ResultObject),
	0,
	(destructor)_mysql_ResultObject_dealloc, /* tp_dealloc */
	0, /*tp_print*/
	(getattrfunc)_mysql_ResultObject_getattr, /* tp_getattr */
	(setattrfunc)_mysql_ResultObject_setattr, /* tp_setattr */
	0, /*tp_compare*/
	(reprfunc)_mysql_ResultObject_repr, /* tp_repr */

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
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_BASETYPE, /* (long) tp_flags */

	_mysql_ResultObject__doc__, /* (char *) tp_doc Documentation string */
	/* call function for all accessible objects */
	(traverseproc)_mysql_ResultObject_traverse, /* tp_traverse */
	/* delete references to contained objects */
	(inquiry)_mysql_ResultObject_clear, /* tp_clear */

	/* rich comparisons */
	0, /* (richcmpfunc) tp_richcompare */

	/* weak reference enabler */
	0, /* (long) tp_weaklistoffset */

	/* Iterators */
	(getiterfunc) _mysql_ResultObject__iter__, /* (getiterfunc) tp_iter */
	(iternextfunc) _mysql_ResultObject_next, /* (iternextfunc) tp_iternext */

	/* Attribute descriptor and subclassing stuff */
	(struct PyMethodDef *)_mysql_ResultObject_methods, /* tp_methods */
	(struct PyMemberDef *)_mysql_ResultObject_memberlist, /*tp_members */
	0, /* (struct getsetlist *) tp_getset; */
	0, /* (struct _typeobject *) tp_base; */
	0, /* (PyObject *) tp_dict */
	0, /* (descrgetfunc) tp_descr_get */
	0, /* (descrsetfunc) tp_descr_set */
	0, /* (long) tp_dictoffset */
	(initproc)_mysql_ResultObject_Initialize, /* tp_init */
	NULL, /* tp_alloc */
	NULL, /* tp_new */
	NULL, /* tp_free Low-level free-memory routine */
	0, /* (PyObject *) tp_bases */
	0, /* (PyObject *) tp_mro method resolution order */
	0, /* (PyObject *) tp_defined */
};
