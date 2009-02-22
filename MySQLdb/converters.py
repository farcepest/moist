"""
MySQLdb type conversion module
------------------------------

This module handles all the type conversions for MySQL. If the default type
conversions aren't what you need, you can make your own. The dictionary
conversions maps some kind of type to a conversion function which returns the
corresponding value:

Key: FIELD_TYPE.* (from MySQLdb.constants)

Conversion function:

    Arguments: string

    Returns: Python object

Key: Python type object (from types) or class

Conversion function:

    Arguments: Python object of indicated type or class AND 
               conversion dictionary

    Returns: SQL literal value

    Notes: Most conversion functions can ignore the dictionary, but it is a
    required parameter. It is necessary for converting things like sequences
    and instances.

Don't modify conversions if you can avoid it. Instead, make copies
(with the copy() method), modify the copies, and then pass them to
MySQL.connect().

"""

from _mysql import string_literal, escape_sequence, escape_dict, NULL
from MySQLdb.constants import FIELD_TYPE, FLAG
from MySQLdb.times import datetime_to_sql, timedelta_to_sql, \
     timedelta_or_None, datetime_or_None, date_or_None, \
     mysql_timestamp_converter
from types import InstanceType
import array
import datetime
from decimal import Decimal

__revision__ = "$Revision$"[11:-2]
__author__ = "$Author$"[9:-2]

def bool_to_sql(boolean, conv):
    """Convert a Python bool to an SQL literal."""
    return str(int(boolean))

def SET_to_Set(value):
    """Convert MySQL SET column to Python set."""
    return set([ i for i in value.split(',') if i ])

def Set_to_sql(value, conv):
    """Convert a Python set to an SQL literal."""
    return string_literal(','.join(value), conv)

def object_to_sql(obj, conv):
    """Convert something into a string via str()."""
    return str(obj)

def unicode_to_sql(value, conv):
    """Convert a unicode object to a string using the default encoding.
    This is only used as a placeholder for the real function, which
    is connection-dependent."""
    assert isinstance(value, unicode)
    return value.encode()

def float_to_sql(value, conv):
    return '%.15g' % value

def None_to_sql(value, conv):
    """Convert None to NULL."""
    return NULL # duh

def object_to_quoted_sql(obj, conv):
    """Convert something into a SQL string literal.  If using
    MySQL-3.23 or newer, string_literal() is a method of the
    _mysql.MYSQL object, and this function will be overridden with
    that method when the connection is created."""

    return string_literal(obj, conv)

def instance_to_sql(obj, conv):
    """Convert an Instance to a string representation.  If the __str__()
    method produces acceptable output, then you don't need to add the
    class to conversions; it will be handled by the default
    converter. If the exact class is not found in conv, it will use the
    first class it can find for which obj is an instance.
    """
    if obj.__class__ in conv:
        return conv[obj.__class__](obj, conv)
    classes = [ key for key in conv.keys()
                if isinstance(obj, key) ]
    if not classes:
        return conv[types.StringType](obj, conv)
    conv[obj.__class__] = conv[classes[0]]
    return conv[classes[0]](obj, conv)

def char_array(obj):
    return array.array('c', obj)

def array_to_sql(obj, conv):
    return object_to_quoted_sql(obj.tostring(), conv)

conversions = {
    int: object_to_sql,
    long: object_to_sql,
    float: float_to_sql,
    type(None): None_to_sql,
    tuple: escape_sequence,
    list: escape_sequence,
    dict: escape_dict,
    InstanceType: instance_to_sql,
    array.array: array_to_sql,
    unicode: unicode_to_sql,
    object: instance_to_sql,
    bool: bool_to_sql,
    datetime.datetime: datetime_to_sql,
    datetime.timedelta: timedelta_to_sql,
    set: Set_to_sql,
    str: object_to_quoted_sql, # default

    FIELD_TYPE.TINY: int,
    FIELD_TYPE.SHORT: int,
    FIELD_TYPE.LONG: int,
    FIELD_TYPE.FLOAT: float,
    FIELD_TYPE.DOUBLE: float,
    FIELD_TYPE.DECIMAL: Decimal,
    FIELD_TYPE.NEWDECIMAL: Decimal,
    FIELD_TYPE.LONGLONG: int,
    FIELD_TYPE.INT24: int,
    FIELD_TYPE.YEAR: int,
    FIELD_TYPE.SET: SET_to_Set,
    FIELD_TYPE.TIMESTAMP: mysql_timestamp_converter,
    FIELD_TYPE.DATETIME: datetime_or_None,
    FIELD_TYPE.TIME: timedelta_or_None,
    FIELD_TYPE.DATE: date_or_None,
    FIELD_TYPE.BLOB: [
        (FLAG.BINARY, str),
        ],
    FIELD_TYPE.STRING: [
        (FLAG.BINARY, str),
        ],
    FIELD_TYPE.VAR_STRING: [
        (FLAG.BINARY, str),
        ],
    FIELD_TYPE.VARCHAR: [
        (FLAG.BINARY, str),
        ],
    }




