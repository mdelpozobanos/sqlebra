"""
SQLebra serves as an interface between python variables and an SQL database.

Database structure
==================

+ Table VARIABLES: Contains all variables defined by the user in the database.
+ Table OBJECTS: Contains the actual values stored in the database.
+ Table ITEMS: Contains items in nested variables (lists, tuples, etc) -- see section "Nested objects" below.

Across tables, a unique identifier (`id`) can be used to link information pertaining to the same variable/object.

TODO: Nested and space efficient should go somewhere else

Nested objects
==============

Python nested variables (e.g. list, tuple and dict) are stored as a root row in table OBJECTS, one row per item in table
ITEMS (all sharing the same `id`) and one row per item in table OBJECTS (each with column `OBJECTS.id` =
`ITEMS.children_id`).

Space efficient
===============

If a new value (i.e. a now row for table OBJECTS) has already be previously defined in the database (i.e. there is a
row in table OBJECTS with the same value), SQLebra can point to the existing row using its `id` to save space.

TODO: Review multiprocessing

Multiprocessing
===============

SQLebra supports pickle and therefore supports multiprocessing. However, it is important to note that the database
connection is not shared across processes: each process instantiates its own connection at the start, and the connection
is closed when the process is killed. Therefore changes to the SQLebra database must be committed before the process is
killed.

The suggestion is to design the paralellized task as a contained database transaction.

TODO: Review example

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
__version__ = '0.3.0'
from contextlib import nullcontext


class SQLebraDatabase:
    """Base class used to identify SQLebra databases."""
    pass


class SQLebraObject:
    """Base class used to identify SQLebra objects."""
    pass


def issqlebra(x):
    """Return True if the passed variable `x` is an SQLebra class."""
    return isinstance(x, SQLebraObject) or isinstance(x, SQLebraDatabase)


def transaction(x):
    """
    Return a context transaction for `x` if `x` is an SQLebra object and an empty context if `x` is not an SQLebra
    object. This can be used to easily support python and SQLebra objects in code.
    """
    if issqlebra(x):
        return x._db.Transaction()
    else:
        return nullcontext()


def py(x):
    """
    Return the python value of `x`. If `x` is not an SQLebra object, it returns `x` itself.  This can be used to
    easily support python and SQLebra objects in code.
    """
    if issqlebra(x):
        return x.py
    else:
        return x


def register_class(sqlebra_class, single_value):
    """Register an SQLebra class into the library.

    User defined SQLebra classes need to be registered on SQLebra to be recognised by the library.

    Parameters
    ----------
    sqlebra_class: class
        An SQLebra object class (i.e. inheriting from `sqlebra.object` or one of its childs (i.e.
        `sqlebra.object.single`)
    single_value: bool
        If true, `sqlebra_class` will be added as a single value class. Else, it will be added as a multi value class.

    See also
    --------
    `sqlebra.py2sql`
    """
    import py2sql
    if single_value:
        py2sql.py2sql_single[sqlebra_class.pyclass] = sqlebra_class
        py2sql.py2sql_single_str[sqlebra_class.pyclass.__name__] = sqlebra_class
    else:
        py2sql.py2sql_multi[sqlebra_class.pyclass] = sqlebra_class
        py2sql.py2sql_multi_str[sqlebra_class.pyclass.__name__] = sqlebra_class


class __FormatGuide:
    """Toy class to showcase the recommended structuring of classes. 
    
    You may copy&paste the code structure shown here when you start writing a new class and fill in the spaces (i.e.
    replace `pass` with methods/properties definitions where necessary).
    
    If the class inherits from another class (e.g. from `Database`), you may start by copying and pasting the "TBD" 
    section of the parent class class, including the class name heading (i.e. the title between '# =-=-=...'; e.g.
    `Database`) so that it is easy to identify where the method/property originated from. You can then delete the
    methods/properties that are not defined in the current class and define the remaining ones. If the class has
    multiple inheriting levels or inherits from multiple classes, you will have multiple inherited TDB sections. Once
    this is done, you can copy&paste the code structure shown here and complete it for the current class.
    """

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # __FormatGuide
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [__FormatGuide] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined/overloaded in inheriting classes

    # [__FormatGuide; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pass

    # [__FormatGuide; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [__FormatGuide; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # [__FormatGuide] FD
    # ==================================================================================================================
    # Methods/properties fully defined here. They can be overloaded by inheriting classes

    # [__FormatGuide; FD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pass

    # [__FormatGuide; FD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [__FormatGuide; FD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass