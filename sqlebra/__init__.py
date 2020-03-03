"""
SQLebra serves as an interface between python variables and an SQL database.

Database structure
==================

An SQLebra database is represented by a single table with columns:

    + id: Unique identifier for each variable
    + name: Variable name
    + class: Name of the variable's python class
    + key: Text index for the current row (see "Nested objects" below).
    + ind: Numeric index for the current row (see "Nested objects" below).
    + bool_val: Boolean value
    + int_val: Integer value
    + real_val: Real value
    + txt_val: Text value
    + child_id: Identifier of the rows child variable (see "Nested objects" below).
    + root: 1 signals the main variable row.
    + user_defined: 1 signals a user defined variable.

Nested objects
==============

Python nested variables (e.g. list, tuple and dict) are represented as a root row (similar to a header) and a row for
each of element. Columns 'key' and 'ind' are used to indexate nested objects.

When a value of a nested object is another nested object, a references to such object is specified in the 'child_id'
column.

Example
=======

from sqlebra.sqlite import SQLiteDB

# Save an integers and a dictionary to an SQL file database
with SQLiteDB('filename.db') as db:
    db['x'] = 10
    db['my_dict'] = {'a': 10, 'b': 20}

# Retrieve variables from an SQL file database
with SQLiteDB('filename.db') as db:
    x = db['x'].py
    my_dict = db['my_dict'].py

"""

__version__ = '1.1.2'
