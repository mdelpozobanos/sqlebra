"""Variables defining the correspondence between python and SQLebra objects.

`py2sql_single`: dict{python class: SQLebra class}
    Dictionary with single value python classes (e.g. int, float, etc) as keys and the corresponding SQLebra class as
    values.
`py2sql_single_str`: dict{str: SQLebra class}
    Dictionary with the name of single value python classes (e.g. int, float, etc) as keys and the corresponding SQLebra
    class as values.
`py2sql_nested`: dict{python class: SQLebra class}
    Dictionary with multi value python classes as keys (e.g. list, tuple, etc) and the corresponding SQLebra class as
    values.
and `py2sql_nested_str`: dict{str: SQLebra class}
    Dictionary with the name of multi value python classes as keys (e.g. list, tuple, etc) and the corresponding SQLebra
    class as values.

Example
-------
User defined SQLebra classes should be added to the relevant dictionaries for SQLebra to identify them. This can be done
manually or through `sqlite.register_class`
"""

from sqlebra.object.python import NoneType_
from sqlebra.object.python import int_
from sqlebra.object.python import float_
from sqlebra.object.python import bool_
from sqlebra.object.python import str_
from sqlebra.object.python import list_
from sqlebra.object.python import tuple_
from sqlebra.object.python import dict_


py2sql = {}
py2sql_str = {}
for c in (
        # constants
        bool_, NoneType_,
        # numeric
        int_, float_,
        # text
        str_,
        # sequence
        list_, tuple_,
        # mapping
        dict_):
    py2sql[c.pyclass] = c
    py2sql_str[c.pyclass.__name__] = c
