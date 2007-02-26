"""times module

This module provides some Date and Time classes for dealing with MySQL data.

Use Python datetime module to handle date and time columns."""

__revision__ = "$ Revision: $"[11:-2]

from time import localtime
from datetime import date, datetime, time, timedelta
from _mysql import string_literal

# These are required for DB-API (PEP-249)
Date = date
Time = time
TimeDelta = timedelta
Timestamp = datetime

def DateFromTicks(ticks):
    """Convert UNIX ticks into a date instance.
    
      >>> DateFromTicks(1172466380)
      datetime.date(2007, 2, 25)
    
    """
    return date(*localtime(ticks)[:3])

def TimeFromTicks(ticks):
    """Convert UNIX ticks into a time instance.
    
      >>> TimeFromTicks(1172466380)
      datetime.time(23, 6, 20)
    
    """
    return time(*localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
    """Convert UNIX ticks into a datetime instance.
    
      >>> TimestampFromTicks(1172466380)
      datetime.datetime(2007, 2, 25, 23, 6, 20)
    
    """
    return datetime(*localtime(ticks)[:6])

format_TIME = format_DATE = str

def format_TIMEDELTA(obj):
    """Format a TIMEDELTA as a string.
    
      >>> format_TIMEDELTA(timedelta(seconds=-86400))
      '-1 00:00:00'
      >>> format_TIMEDELTA(timedelta(hours=73, minutes=15, seconds=32))
      '3 01:15:32'
      
    """
    seconds = int(obj.seconds) % 60
    minutes = int(obj.seconds / 60) % 60
    hours = int(obj.seconds / 3600) % 24
    return '%d %02d:%02d:%02d' % (obj.days, hours, minutes, seconds)

def format_TIMESTAMP(obj):
    return obj.strftime("%Y-%m-%d %H:%M:%S")

def datetime_or_None(obj):
    if ' ' in obj:
        sep = ' '
    elif 'T' in obj:
        sep = 'T'
    else:
        return date_or_None(obj)

    try:
        ymd, hms = obj.split(sep, 1)
        return datetime(*[ int(x) for x in ymd.split('-')+hms.split(':') ])
    except ValueError:
        return date_or_None(obj)

def timedelta_or_None(obj):
    from math import modf
    try:
        hours, minutes, seconds = obj.split(':')
        tdelta = timedelta(hours=int(hours), minutes=int(minutes), seconds=int(seconds),
                           microseconds=int(modf(float(seconds))[0]*1000000))
        if hours < 0:
            return -tdelta
        else:
            return tdelta
    except ValueError:
        return None

def time_or_None(obj):
    from math import modf
    try:
        hour, minute, second = obj.split(':')
        return time(hour=int(hour), minute=int(minute), second=int(second),
                    microsecond=int(modf(float(second))[0]*1000000))
    except ValueError:
        return None

def date_or_None(obj):
    try:
        return date(*[ int(x) for x in obj.split('-',2)])
    except ValueError:
        return None

def datetime_to_sql(obj, conv):
    """Format a DateTime object as an ISO timestamp."""
    return string_literal(format_TIMESTAMP(obj), conv)
    
def timedelta_to_sql(obj, conv):
    """Format a timedelta as an SQL literal.
    
      >>> timedelta_to_sql(timedelta(hours=73, minutes=15, seconds=32), {})
      "'3 01:15:32'"
      
    """
    return string_literal(format_TIMEDELTA(obj), conv)

def mysql_timestamp_converter(timestamp):
    """Convert a MySQL TIMESTAMP to a Timestamp object.

    MySQL>4.1 returns TIMESTAMP in the same format as DATETIME:
    
      >>> mysql_timestamp_converter('2007-02-25 22:32:17')
      datetime.datetime(2007, 2, 25, 22, 32, 17)
    
    MySQL<4.1 uses a big string of numbers:
    
      >>> mysql_timestamp_converter('20070225223217')
      datetime.datetime(2007, 2, 25, 22, 32, 17)
    
    Illegal values are returned as None:
    
      >>> print mysql_timestamp_converter('2007-02-31 22:32:17')
      None
      >>> print mysql_timestamp_converter('00000000000000')
      None
      
    """
    if timestamp[4] == '-':
        return datetime_or_None(timestamp)
    timestamp += "0"*(14-len(timestamp)) # padding
    year, month, day, hour, minute, second = \
        int(timestamp[:4]), int(timestamp[4:6]), int(timestamp[6:8]), \
        int(timestamp[8:10]), int(timestamp[10:12]), int(timestamp[12:14])
    try:
        return datetime(year, month, day, hour, minute, second)
    except ValueError:
        return None

if __name__ == "__main__":
    import doctest
    doctest.testmod()
    