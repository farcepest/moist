"""
MySQLdb type conversion module
------------------------------



"""

from _mysql import NULL
from MySQLdb.constants import FIELD_TYPE, FLAG
from MySQLdb.times import datetime_to_sql, timedelta_to_sql, \
     timedelta_or_None, datetime_or_None, date_or_None, \
     mysql_timestamp_converter
from types import InstanceType
import array
import datetime
from decimal import Decimal
from itertools import izip

__revision__ = "$Revision$"[11:-2]
__author__ = "$Author$"[9:-2]

def bool_to_sql(connection, boolean):
    """Convert a Python bool to an SQL literal."""
    return str(int(boolean))

def SET_to_Set(value):
    """Convert MySQL SET column to Python set."""
    return set([ i for i in value.split(',') if i ])

def Set_to_sql(connection, value):
    """Convert a Python set to an SQL literal."""
    return connection.string_literal(','.join(value))

def object_to_sql(connection, obj):
    """Convert something into a string via str().
    The result will not be quoted."""
    return connection.escape_string(str(obj))

def unicode_to_sql(connection, value):
    """Convert a unicode object to a string using the connection encoding."""
    return connection.string_literal(value.encode(connection.character_set_name()))

def float_to_sql(connection, value):
    return '%.15g' % value

def None_to_sql(connection, value):
    """Convert None to NULL."""
    return NULL # duh

def object_to_quoted_sql(connection, obj):
    """Convert something into a SQL string literal."""
    if hasattr(obj, "__unicode__"):
        return unicode_to_sql(connection, obj)
    return connection.string_literal(str(obj))

def instance_to_sql(connection, obj):
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

def array_to_sql(connection, obj):
    return connection.string_literal(obj.tostring())

simple_type_encoders = {
    int: object_to_sql,
    long: object_to_sql,
    float: float_to_sql,
    type(None): None_to_sql,
    unicode: unicode_to_sql,
    object: instance_to_sql,
    bool: bool_to_sql,
    datetime.datetime: datetime_to_sql,
    datetime.timedelta: timedelta_to_sql,
    set: Set_to_sql,
    str: object_to_quoted_sql, # default
    }

# This is for MySQL column types that can be converted directly
# into Python types without having to look at metadata (flags,
# character sets, etc.). This should always be used as the last
# resort.
simple_field_decoders = {
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
    }

# Decoder protocol
# Each decoder is passed a cursor object and a field object.
# The decoder returns a single value:
# * A callable that given an SQL value, returns a Python object.
# This can be as simple as int or str, etc. If the decoder
# returns None, this decoder will be ignored and the next decoder
# on the stack will be checked.

def default_decoder(field):
    return str

def default_encoder(value):
    return object_to_quoted_sql
    
def simple_decoder(field):
    return simple_field_decoders.get(field.type, None)

def simple_encoder(value):
    return simple_type_encoders.get(type(value), None)

character_types = [
    FIELD_TYPE.BLOB, 
    FIELD_TYPE.STRING,
    FIELD_TYPE.VAR_STRING,
    FIELD_TYPE.VARCHAR,
    ]

def character_decoder(field):
    if field.type not in character_types:
        return None
    if field.charsetnr == 63: # BINARY
        return str
    
    charset = field.result.connection.character_set_name()
    def char_to_unicode(s):
        if s is None:
            return s
        return s.decode(charset)
    
    return char_to_unicode

default_decoders = [
    character_decoder,
    simple_decoder,
    default_decoder,
    ]

default_encoders = [
    simple_encoder,
    default_encoder,
    ]

def get_codec(field, codecs):
    for c in codecs:
        func = c(field)
        if func:
            return func
    # the default codec is guaranteed to work

def iter_row_decoder(decoders, row):
    if row is None:
        return None
    return ( d(col) for d, col in izip(decoders, row) )

def tuple_row_decoder(decoders, row):
    if row is None:
        return None
    return tuple(iter_row_decoder(decoders, row))






