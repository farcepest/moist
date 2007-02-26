#!/bin/sh
echo '"""
MySQL ER Constants
------------------

These constants are error codes for the bulk of the error conditions
that may occur.

"""
__revision__ = "$Revision$"[11:-2]
__author__ = "$Author$"[9:-2]
'
sed -e 's|/\* \(.*\) \*/|# \1|' -e 's/^#define \(ER_\)*//' -e 's/ \([0-9][0-9]*\)/ = \1/' </usr/include/mysql/mysqld_error.h
