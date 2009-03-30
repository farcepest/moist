"""
times module
------------

This module provides some help functions for dealing with MySQL data.
Most of these you will not have to use directly.

Uses Python datetime module to handle time-releated columns."""

__revision__ = "$Revision$"[11:-2]
__author__ = "$Author$"[9:-2]

from time import localtime
from datetime import date, datetime, time, timedelta

# These are required for DB-API (PEP-249)
Date = date
Time = time
TimeDelta = timedelta
Timestamp = datetime

def DateFromTicks(ticks):
    """Convert UNIX ticks into a date instance.
    
      >>> DateFromTicks(1172466380)
      datetime.date(2007, 2, 25)
      >>> DateFromTicks(0)
      datetime.date(1969, 12, 31)
      >>> DateFromTicks(2**31-1)
      datetime.date(2038, 1, 18)

    This is a standard DB-API constructor.
    """
    return date(*localtime(ticks)[:3])

def TimeFromTicks(ticks):
    """Convert UNIX ticks into a time instance.
    
      >>> TimeFromTicks(1172466380)
      datetime.time(23, 6, 20)
      >>> TimeFromTicks(0)
      datetime.time(18, 0)
      >>> TimeFromTicks(2**31-1)
      datetime.time(21, 14, 7)
    
    This is a standard DB-API constructor.
    """
    return time(*localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
    """Convert UNIX ticks into a datetime instance.
    
      >>> TimestampFromTicks(1172466380)
      datetime.datetime(2007, 2, 25, 23, 6, 20)
      >>> TimestampFromTicks(0)
      datetime.datetime(1969, 12, 31, 18, 0)
      >>> TimestampFromTicks(2**31-1)
      datetime.datetime(2038, 1, 18, 21, 14, 7)
    
    This is a standard DB-API constructor.
    """
    return datetime(*localtime(ticks)[:6])

def timedelta_to_str(obj):
    """Format a timedelta as a string.
    
      >>> timedelta_to_str(timedelta(seconds=-86400))
      '-1 00:00:00'
      >>> timedelta_to_str(timedelta(hours=73, minutes=15, seconds=32))
      '3 01:15:32'
      
    """
    seconds = int(obj.seconds) % 60
    minutes = int(obj.seconds / 60) % 60
    hours = int(obj.seconds / 3600) % 24
    return '%d %02d:%02d:%02d' % (obj.days, hours, minutes, seconds)

def datetime_to_str(obj):
    """Convert a datetime to an ISO-format string.
    
      >>> datetime_to_str(datetime(2007, 2, 25, 23, 6, 20))
      '2007-02-25 23:06:20'
    
    """
    return obj.strftime("%Y-%m-%d %H:%M:%S")

def datetime_or_None(obj):
    """Returns a DATETIME or TIMESTAMP column value as a datetime object:
    
      >>> datetime_or_None('2007-02-25 23:06:20')
      datetime.datetime(2007, 2, 25, 23, 6, 20)
      >>> datetime_or_None('2007-02-25T23:06:20')
      datetime.datetime(2007, 2, 25, 23, 6, 20)
    
    Illegal values are returned as None:
    
      >>> datetime_or_None('2007-02-31T23:06:20') is None
      True
      >>> datetime_or_None('0000-00-00 00:00:00') is None
      True
   
    """
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
    """Returns a TIME column as a timedelta object:

      >>> timedelta_or_None('25:06:17')
      datetime.timedelta(1, 3977)
      >>> timedelta_or_None('-25:06:17')
      datetime.timedelta(-2, 83177)
      
    Illegal values are returned as None:
    
      >>> timedelta_or_None('random crap') is None
      True
   
    Note that MySQL always returns TIME columns as (+|-)HH:MM:SS, but
    can accept values as (+|-)DD HH:MM:SS. The latter format will not
    be parsed correctly by this function.
    """
    from math import modf
    try:
        hours, minutes, seconds = obj.split(':')
        tdelta = timedelta(
            hours = int(hours),
            minutes = int(minutes),
            seconds = int(seconds),
            microseconds = int(modf(float(seconds))[0]*1000000),
            )
        if hours < 0:
            return -tdelta
        else:
            return tdelta
    except ValueError:
        return None

def time_or_None(obj):
    """Returns a TIME column as a time object:

      >>> time_or_None('15:06:17')
      datetime.time(15, 6, 17)
      
    Illegal values are returned as None:
 
      >>> time_or_None('-25:06:17') is None
      True
      >>> time_or_None('random crap') is None
      True
   
    Note that MySQL always returns TIME columns as (+|-)HH:MM:SS, but
    can accept values as (+|-)DD HH:MM:SS. The latter format will not
    be parsed correctly by this function.
    
    Also note that MySQL's TIME column corresponds more closely to
    Python's timedelta and not time. However if you want TIME columns
    to be treated as time-of-day and not a time offset, then you can
    use set this function as the converter for FIELD_TYPE.TIME.
    """
    from math import modf
    try:
        hour, minute, second = obj.split(':')
        return time(hour=int(hour), minute=int(minute), second=int(second),
                    microsecond=int(modf(float(second))[0]*1000000))
    except ValueError:
        return None

def date_or_None(obj):
    """Returns a DATE column as a date object:

      >>> date_or_None('2007-02-26')
      datetime.date(2007, 2, 26)
      
    Illegal values are returned as None:
 
      >>> date_or_None('2007-02-31') is None
      True
      >>> date_or_None('0000-00-00') is None
      True
    
    """
    try:
        return date(*map(int, obj.split('-', 2)))
    except ValueError:
        return None

def datetime_to_sql(connection, obj):
    """Format a DateTime object as an ISO timestamp."""
    return connection.string_literal(datetime_to_str(obj))
    
def timedelta_to_sql(connection, obj):
    """Format a timedelta as an SQL literal."""
    return connection.string_literal(timedelta_to_str(obj))

def mysql_timestamp_converter(timestamp):
    """Convert a MySQL TIMESTAMP to a Timestamp object.

    MySQL >= 4.1 returns TIMESTAMP in the same format as DATETIME:
    
      >>> mysql_timestamp_converter('2007-02-25 22:32:17')
      datetime.datetime(2007, 2, 25, 22, 32, 17)
    
    MySQL < 4.1 uses a big string of numbers:
    
      >>> mysql_timestamp_converter('20070225223217')
      datetime.datetime(2007, 2, 25, 22, 32, 17)
    
    Illegal values are returned as None:
    
      >>> mysql_timestamp_converter('2007-02-31 22:32:17') is None
      True
      >>> mysql_timestamp_converter('00000000000000') is None
      True
      
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
    