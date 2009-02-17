/* -*- mode: C; indent-tabs-mode: t; c-basic-offset: 8; -*- */

#include "_mysql.h"

static char _mysql_FieldObject__doc__[] =
"";

int
_mysql_FieldObject_Initialize(
	_mysql_FieldObject *self,
	PyObject *args,
	PyObject *kwargs)
{
	static char *kwlist[] = {"result", "index", NULL};
	_mysql_ResultObject *result=NULL;
	MYSQL_FIELD *field;
	unsigned int index;
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Oi", kwlist,
					  &result, &index))
		return -1;

	self->index = index;
	field = mysql_fetch_field_direct(result->result, index);
	if (!field) return -1;
	self->field = *field;
	self->result = (PyObject *) result;
	Py_INCREF(result);
	return 0;
}

static int
_mysql_FieldObject_traverse(
	_mysql_FieldObject *self,
	visitproc visit,
	void *arg)
{
	if (self->result)
		return visit(self->result, arg);
	return 0;
}

static int
_mysql_FieldObject_clear(
	_mysql_FieldObject *self)
{
	Py_XDECREF(self->result);
	self->result = NULL;
	return 0;
}

static void
_mysql_FieldObject_dealloc(
	_mysql_FieldObject *self)
{
	PyObject_GC_UnTrack((PyObject *)self);
	_mysql_FieldObject_clear(self);
	MyFree(self);
}

static PyObject *
_mysql_FieldObject_repr(
	_mysql_FieldObject *self)
{
	char buf[300];
	snprintf(buf, 300, "<_mysql.field object at %lx>", (long)self);
	return PyString_FromString(buf);
}

static PyMethodDef _mysql_FieldObject_methods[] = {
	{NULL,              NULL} /* sentinel */
};

static struct PyMemberDef _mysql_FieldObject_memberlist[] = {
	{
		"result",
		T_OBJECT,
		offsetof(_mysql_FieldObject, result),
		RO,
		"Result set"
	},
	{
		"name",
		T_STRING,
		offsetof(_mysql_FieldObject, field.name),
		RO,
		"The name of the field. If the field was given\n\
an alias with an AS clause, the value of name is the alias."
	},
	{
		"org_name",
		T_STRING,
		offsetof(_mysql_FieldObject, field.org_name),
		RO,
		"The name of the field. Aliases are ignored."
	},
	{
		"table",
		T_STRING,
		offsetof(_mysql_FieldObject, field.table),
		RO,
		"The name of the table containing this field,\n\
if it isn't a calculated field. For calculated fields,\n\
the table value is an empty string. If the column is selected from a view,\n\
table names the view. If the table or view was given an alias with an AS clause,\n\
the value of table is the alias.\n"
	},
	{
		"org_table",
		T_STRING,
		offsetof(_mysql_FieldObject, field.org_table),
		RO,
		"The name of the table. Aliases are ignored.\n\
If the column is selected from a view, org_table names the underlying table.\n"
	},
	{
		"db",
		T_STRING,
		offsetof(_mysql_FieldObject, field.db),
		RO,
		"The name of the database that the field comes from.\n\
If the field is a calculated field, db is an empty string."
	},
	{
		"catalog",
		T_STRING,
		offsetof(_mysql_FieldObject, field.catalog),
		RO,
		"The catalog name. This value is always \"def\"."
	},
	{
		"length",
		T_ULONG,
		offsetof(_mysql_FieldObject, field.length),
		RO,
		"The width of the field.\n\
as specified in the table definition.\n"
	},
	{
		"max_length",
		T_ULONG,
		offsetof(_mysql_FieldObject, field.max_length),
		RO,
		"The maximum width of the field for the result set\n\
(the length of the longest field value for the rows actually in the\n\
result set). If you use conn.store_result(), this contains the\n\
maximum length for the field. If you use conn.use_result(),\n\
the value of this variable is zero.\n"
	},
	{
		"decimals",
		T_UINT,
		offsetof(_mysql_FieldObject, field.decimals),
		RO,
		"The number of decimals for numeric fields.\n"
	},
	{
		"charsetnr",
		T_UINT,
		offsetof(_mysql_FieldObject, field.charsetnr),
		RO,
		"The character set number for the field."
	},
	{
		"flags",
		T_UINT,
		offsetof(_mysql_FieldObject, field.flags),
		RO,
		"Different bit-flags for the field.\n\
The bits are enumerated in MySQLdb.constants.FLAG.\n\
The flags value may have zero or more of these bits set.\n"
	},
	{
		"type",
		T_UINT,
		offsetof(_mysql_FieldObject, field.type),
		RO,
		"The type of the field. The type values\n\
are enumerated in MySQLdb.constants.FIELD_TYPE.\n"
	},
	{NULL} /* Sentinel */
};

static PyObject *
_mysql_FieldObject_getattr(
	_mysql_FieldObject *self,
	char *name)
{
	PyObject *res;
	struct PyMemberDef *l;

	res = Py_FindMethod(_mysql_FieldObject_methods, (PyObject *)self, name);
	if (res != NULL)
		return res;
	PyErr_Clear();

	for (l = _mysql_FieldObject_memberlist; l->name != NULL; l++) {
		if (strcmp(l->name, name) == 0)
			return PyMember_GetOne((char *)self, l);
	}

	PyErr_SetString(PyExc_AttributeError, name);
	return NULL;
}

static int
_mysql_FieldObject_setattr(
	_mysql_FieldObject *self,
	char *name,
	PyObject *v)
{
	struct PyMemberDef *l;

	if (v == NULL) {
		PyErr_SetString(PyExc_AttributeError,
				"can't delete attributes");
		return -1;
	}


	for (l = _mysql_FieldObject_memberlist; l->name != NULL; l++)
		if (strcmp(l->name, name) == 0)
			return PyMember_SetOne((char *)self, l, v);

        PyErr_SetString(PyExc_AttributeError, name);
        return -1;
}

PyTypeObject _mysql_FieldObject_Type = {
	PyObject_HEAD_INIT(NULL)
	0,
	"_mysql.field",
	sizeof(_mysql_FieldObject),
	0,
	(destructor)_mysql_FieldObject_dealloc, /* tp_dealloc */
	0, /*tp_print*/
	(getattrfunc)_mysql_FieldObject_getattr, /* tp_getattr */
	(setattrfunc)_mysql_FieldObject_setattr, /* tp_setattr */
	0, /*tp_compare*/
	(reprfunc)_mysql_FieldObject_repr, /* tp_repr */

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
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_BASETYPE,

	_mysql_FieldObject__doc__, /* (char *) tp_doc Documentation string */

	/* call function for all accessible objects */
	(traverseproc)_mysql_FieldObject_traverse, /* tp_traverse */

	/* delete references to contained objects */
	(inquiry)_mysql_FieldObject_clear, /* tp_clear */

	/* rich comparisons */
	0, /* (richcmpfunc) tp_richcompare */

	/* weak reference enabler */
	0, /* (long) tp_weaklistoffset */

	/* Iterators */
	0, /* (getiterfunc) tp_iter */
	0, /* (iternextfunc) tp_iternext */

	/* Attribute descriptor and subclassing stuff */
	(struct PyMethodDef *)_mysql_FieldObject_methods, /* tp_methods */
	(struct PyMemberDef *)_mysql_FieldObject_memberlist, /*tp_members */
	0, /* (struct getsetlist *) tp_getset; */
	0, /* (struct _typeobject *) tp_base; */
	0, /* (PyObject *) tp_dict */
	0, /* (descrgetfunc) tp_descr_get */
	0, /* (descrsetfunc) tp_descr_set */
	0, /* (long) tp_dictoffset */
	(initproc)_mysql_FieldObject_Initialize, /* tp_init */
	NULL, /* tp_alloc */
	NULL, /* tp_new */
	NULL, /* tp_free Low-level free-memory routine */
	0, /* (PyObject *) tp_bases */
	0, /* (PyObject *) tp_mro method resolution order */
	0, /* (PyObject *) tp_defined */
};
