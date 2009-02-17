/* -*- mode: C; indent-tabs-mode: t; c-basic-offset: 8; -*- */

#include "_mysql.h"

static char _mysql_ResultObject__doc__[] =
"result(connection, use=0, converter={}) -- Result set from a query.\n\
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
	static char *kwlist[] = {"connection", "use", "converter", NULL};
	MYSQL_RES *result; 
	_mysql_ConnectionObject *conn=NULL;
	int use=0; 
	PyObject *conv=NULL;
	int n, i;
	MYSQL_FIELD *fields;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|iO", kwlist,
					  &conn, &use, &conv))
		return -1;
	if (!conv) conv = PyDict_New();
	if (!conv) return -1;
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
		self->converter = PyTuple_New(0);
		return 0;
	}
	n = mysql_num_fields(result);
	self->nfields = n;
	if (!(self->converter = PyTuple_New(n))) return -1;
	fields = mysql_fetch_fields(result);
	for (i=0; i<n; i++) {
		PyObject *tmp, *fun;
		tmp = PyInt_FromLong((long) fields[i].type);
		if (!tmp) return -1;
		fun = PyObject_GetItem(conv, tmp);
		Py_DECREF(tmp);
		if (!fun) {
			PyErr_Clear();
			fun = Py_None;
			Py_INCREF(Py_None);
		}
		if (PySequence_Check(fun)) {
			int j, n2=PySequence_Size(fun);
			PyObject *fun2=NULL;
			for (j=0; j<n2; j++) {
				PyObject *t = PySequence_GetItem(fun, j);
				if (!t) continue;
				if (!PyTuple_Check(t)) goto cleanup;
				if (PyTuple_GET_SIZE(t) == 2) {
					long mask;
					PyObject *pmask=NULL;
					pmask = PyTuple_GET_ITEM(t, 0);
					fun2 = PyTuple_GET_ITEM(t, 1);
					if (PyInt_Check(pmask)) {
						mask = PyInt_AS_LONG(pmask);
						if (mask & fields[i].flags) {
							break;
						}
						else {
							continue;
						}
					} else {
						break;
					}
				}
			  cleanup:
				Py_DECREF(t);
			}
			if (!fun2) fun2 = Py_None;
			Py_INCREF(fun2);
			Py_DECREF(fun);
			fun = fun2;
		}
		PyTuple_SET_ITEM(self->converter, i, fun);
	}
	return 0;
}

#if PY_VERSION_HEX >= 0x02020000
static int _mysql_ResultObject_traverse(
	_mysql_ResultObject *self,
	visitproc visit,
	void *arg)
{
	int r;
	if (self->converter) {
		if (!(r = visit(self->converter, arg))) return r;
	}
	if (self->conn)
		return visit(self->conn, arg);
	return 0;
}
#endif

static int _mysql_ResultObject_clear(
	_mysql_ResultObject *self)
{
	Py_XDECREF(self->converter);
	self->converter = NULL;
	Py_XDECREF(self->conn);
	self->conn = NULL;
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

static char _mysql_ResultObject_fields__doc__[] =
"Returns the sequence of 7-tuples required by the DB-API for\n\
the Cursor.description attribute.\n\
";

static PyObject *
_mysql_ResultObject_fields(
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

static PyObject *
_mysql_field_to_python(
	PyObject *converter,
	char *rowitem,
	unsigned long length)
{
	PyObject *v;
	if (rowitem) {
		if (converter != Py_None)
			v = PyObject_CallFunction(converter,
						  "s#",
						  rowitem,
						  (int)length);
		else
			v = PyString_FromStringAndSize(rowitem,
						       (int)length);
		if (!v)
			return NULL;
	} else {
		Py_INCREF(Py_None);
		v = Py_None;
	}
	return v;
}

static PyObject *
_mysql_row_to_tuple(
	_mysql_ResultObject *self,
	MYSQL_ROW row)
{
	unsigned int n, i;
	unsigned long *length;
	PyObject *r, *c;

	n = mysql_num_fields(self->result);
	if (!(r = PyTuple_New(n))) return NULL;
	length = mysql_fetch_lengths(self->result);
	for (i=0; i<n; i++) {
		PyObject *v;
		c = PyTuple_GET_ITEM(self->converter, i);
		v = _mysql_field_to_python(c, row[i], length[i]);
		if (!v) goto error;
		PyTuple_SET_ITEM(r, i, v);
	}
	return r;
  error:
	Py_XDECREF(r);
	return NULL;
}

static PyObject *
_mysql_row_to_dict(
	_mysql_ResultObject *self,
	MYSQL_ROW row)
{
	unsigned int n, i;
	unsigned long *length;
	PyObject *r, *c;
        MYSQL_FIELD *fields;

	n = mysql_num_fields(self->result);
	if (!(r = PyDict_New())) return NULL;
	length = mysql_fetch_lengths(self->result);
        fields = mysql_fetch_fields(self->result);
	for (i=0; i<n; i++) {
		PyObject *v;
		c = PyTuple_GET_ITEM(self->converter, i);
		v = _mysql_field_to_python(c, row[i], length[i]);
		if (!v) goto error;
		if (!PyMapping_HasKeyString(r, fields[i].name)) {
			PyMapping_SetItemString(r, fields[i].name, v);
		} else {
			int len;
			char buf[256];
			strncpy(buf, fields[i].table, 256);
			len = strlen(buf);
			strncat(buf, ".", 256-len);
			len = strlen(buf);
			strncat(buf, fields[i].name, 256-len);
			PyMapping_SetItemString(r, buf, v);
		}
		Py_DECREF(v);
	}
	return r;
  error:
	Py_XDECREF(r);
	return NULL;
}

static PyObject *
_mysql_row_to_dict_old(
	_mysql_ResultObject *self,
	MYSQL_ROW row)
{
	unsigned int n, i;
	unsigned long *length;
	PyObject *r, *c;
        MYSQL_FIELD *fields;

	n = mysql_num_fields(self->result);
	if (!(r = PyDict_New())) return NULL;
	length = mysql_fetch_lengths(self->result);
        fields = mysql_fetch_fields(self->result);
	for (i=0; i<n; i++) {
		PyObject *v;
		c = PyTuple_GET_ITEM(self->converter, i);
		v = _mysql_field_to_python(c, row[i], length[i]);
		if (!v) goto error;
		{
			int len=0;
			char buf[256]="";
			if (strlen(fields[i].table)) {
				strncpy(buf, fields[i].table, 256);
				len = strlen(buf);
				strncat(buf, ".", 256-len);
				len = strlen(buf);
			}
			strncat(buf, fields[i].name, 256-len);
			PyMapping_SetItemString(r, buf, v);
		}
		Py_DECREF(v);
	}
	return r;
  error:
	Py_XDECREF(r);
	return NULL;
}

typedef PyObject *_PYFUNC(_mysql_ResultObject *, MYSQL_ROW);

int
_mysql__fetch_row(
	_mysql_ResultObject *self,
	PyObject **r,
	int skiprows,
	int maxrows,
	_PYFUNC *convert_row)
{
	unsigned int i;
	MYSQL_ROW row;

	for (i = skiprows; i<(skiprows+maxrows); i++) {
		PyObject *v;
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
			if (_PyTuple_Resize(r, i) == -1) goto error;
			break;
		}
		v = convert_row(self, row);
		if (!v) goto error;
		PyTuple_SET_ITEM(*r, i, v);
	}
	return i-skiprows;
  error:
	return -1;
}

static char _mysql_ResultObject_fetch_row__doc__[] =
"fetch_row([maxrows, how]) -- Fetches up to maxrows as a tuple.\n\
The rows are formatted according to how:\n\
\n\
    0 -- tuples (default)\n\
    1 -- dictionaries, key=column or table.column if duplicated\n\
    2 -- dictionaries, key=table.column\n\
";

static PyObject *
_mysql_ResultObject_fetch_row(
	_mysql_ResultObject *self,
	PyObject *args,
	PyObject *kwargs)
{
	typedef PyObject *_PYFUNC(_mysql_ResultObject *, MYSQL_ROW);
	static char *kwlist[] = { "maxrows", "how", NULL };
	static _PYFUNC *row_converters[] =
	{
		_mysql_row_to_tuple,
		_mysql_row_to_dict,
		_mysql_row_to_dict_old
	};
	_PYFUNC *convert_row;
	unsigned int maxrows=1, how=0, skiprows=0, rowsadded;
	PyObject *r=NULL;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|ii:fetch_row", kwlist,
					 &maxrows, &how))
		return NULL;
	check_result_connection(self);
	if (how < 0 || how >= sizeof(row_converters)) {
		PyErr_SetString(PyExc_ValueError, "how out of range");
		return NULL;
	}
	convert_row = row_converters[how];
	if (maxrows) {
		if (!(r = PyTuple_New(maxrows))) goto error;
		rowsadded = _mysql__fetch_row(self, &r, skiprows, maxrows,
				convert_row);
		if (rowsadded == -1) goto error;
	} else {
		if (self->use) {
			maxrows = 1000;
			if (!(r = PyTuple_New(maxrows))) goto error;
			while (1) {
				rowsadded = _mysql__fetch_row(self, &r, skiprows,
						maxrows, convert_row);
				if (rowsadded == -1) goto error;
				skiprows += rowsadded;
				if (rowsadded < maxrows) break;
				if (_PyTuple_Resize(&r, skiprows + maxrows) == -1)
				        goto error;
			}
		} else {
			/* XXX if overflow, maxrows<0? */
			maxrows = (int) mysql_num_rows(self->result);
			if (!(r = PyTuple_New(maxrows))) goto error;
			rowsadded = _mysql__fetch_row(self, &r, 0,
					maxrows, convert_row);
			if (rowsadded == -1) goto error;
		}
	}
	return r;
  error:
	Py_XDECREF(r);
	return NULL;
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
	mysql_free_result(self->result);
	_mysql_ResultObject_clear(self);
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
		"describe",
		(PyCFunction)_mysql_ResultObject_describe,
		METH_NOARGS,
		_mysql_ResultObject_describe__doc__
	},
	{
		"fields",
		(PyCFunction)_mysql_ResultObject_fields,
		METH_NOARGS,
		_mysql_ResultObject_fields__doc__
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
		"converter",
		T_OBJECT,
		offsetof(_mysql_ResultObject, converter),
		RO,
		"Type conversion mapping"
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
#if PY_VERSION_HEX < 0x02020000
	Py_TPFLAGS_DEFAULT, /* (long) tp_flags */
#else
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_BASETYPE,
#endif
	
	_mysql_ResultObject__doc__, /* (char *) tp_doc Documentation string */
#if PY_VERSION_HEX >= 0x02000000	
	/* Assigned meaning in release 2.0 */
#if PY_VERSION_HEX >= 0x02020000
	/* call function for all accessible objects */
	(traverseproc) _mysql_ResultObject_traverse, /* tp_traverse */
	
	/* delete references to contained objects */
	(inquiry) _mysql_ResultObject_clear, /* tp_clear */
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
#endif /* python 2.2 */
#endif /* python 2.1 */
#endif /* python 2.0 */
};
